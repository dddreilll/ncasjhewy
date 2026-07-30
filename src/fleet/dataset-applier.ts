import { Logger } from '@nestjs/common';
import { mkdir, readFile, rename, rm, writeFile } from 'fs/promises';
import { dirname, join } from 'path';
import {
  computeContentHash,
  PartialPayload,
  SyncMessage,
} from '../contracts/data-sync';

export interface DatasetApplier {
  applySnapshot(message: SyncMessage): Promise<void>;
  applyPartial(message: SyncMessage): Promise<void>;
  deleteDataset(datasetType: string): Promise<void>;
  deleteAllDatasets(): Promise<void>;
}

interface StoredDataset {
  datasetType: string;
  version: number;
  contentHash: string;
  appliedAt: string;
  records: unknown;
}

/**
 * Applies messages to ONE store's local dataset files
 * (`<dataDir>/<storeCode>/datasets/<type>.json`). Verifies the message's
 * contentHash before applying so corrupted/tampered payloads are rejected and
 * acked FAILED. A real POS would write into its local database instead of
 * files; appliers must stay idempotent — the consumer's version guard filters
 * replays, but applies can be retried after a crash.
 *
 * `datasets/<type>.json` (the dashboard's "Applied Dataset" preview) holds
 * the **reconstructed, current record set** — a SNAPSHOT's full payload as
 * received, or a PARTIAL's diff already merged into what came before. That's
 * the useful "what does this store have right now" view.
 *
 * `wire-messages/<type>.json` (the dashboard's "As received" preview) holds
 * the **exact, verbatim `SyncMessage` as received off RabbitMQ** — envelope
 * and all, unmerged, unreshaped, nothing added or dropped. Overwritten on
 * every apply, same as the reconstructed file — only the latest wire message
 * is kept, not a history. Also logged in full on every apply, so it's
 * visible via `pm2 logs` too, not just the dashboard.
 */
export class FileDatasetApplier implements DatasetApplier {
  constructor(
    private readonly datasetsDir: string,
    private readonly wireMessagesDir: string,
    private readonly logger: Logger,
  ) {}

  static forStore(
    dataDir: string,
    storeCode: string,
    logger: Logger,
  ): FileDatasetApplier {
    return new FileDatasetApplier(
      join(dataDir, storeCode, 'datasets'),
      join(dataDir, storeCode, 'wire-messages'),
      logger,
    );
  }

  async applySnapshot(message: SyncMessage): Promise<void> {
    this.verifyHash(message);
    await this.write(message, message.payload);
    await this.writeWireMessage(message);
    this.logger.log(
      `APPLIED SNAPSHOT ${message.datasetType} v${message.version} (schema ${message.schemaVersion})`,
    );
  }

  async applyPartial(message: SyncMessage): Promise<void> {
    this.verifyHash(message);

    const current = await this.read(message.datasetType);
    const partial = message.payload as PartialPayload;

    let merged: unknown;
    if (partial.recordsField) {
      // Wrapper payloads (e.g. { service, createdBy, paymentTypes: [...] }):
      // merge inside partial.recordsField, leave the other wrapper fields as-is.
      const wrapper = current?.records as Record<string, unknown> | undefined;
      const array = wrapper?.[partial.recordsField];
      if (!wrapper || !Array.isArray(array)) {
        throw new Error(
          `Cannot apply partial for ${message.datasetType}: stored snapshot has no "${partial.recordsField}" array`,
        );
      }
      merged = {
        ...wrapper,
        [partial.recordsField]: this.mergeRecords(
          array,
          partial,
          message.datasetType,
        ),
      };
    } else {
      if (!current || !Array.isArray(current.records)) {
        throw new Error(
          `Cannot apply partial for ${message.datasetType}: no array snapshot on disk to merge into`,
        );
      }
      merged = this.mergeRecords(
        current.records as Record<string, unknown>[],
        partial,
        message.datasetType,
      );
    }

    await this.write(message, merged);
    await this.writeWireMessage(message);
    this.logger.log(
      `APPLIED PARTIAL ${message.datasetType} v${message.previousVersion} -> v${message.version} (${partial.upserts?.length ?? 0} upserts, ${partial.deletes?.length ?? 0} deletes)`,
    );
  }

  private mergeRecords(
    current: Record<string, unknown>[],
    partial: PartialPayload,
    datasetType: string,
  ): Record<string, unknown>[] {
    const records = new Map<string | number, Record<string, unknown>>();
    for (const record of current) {
      records.set(this.keyOf(record, partial, datasetType), record);
    }
    for (const record of partial.upserts ?? []) {
      records.set(this.keyOf(record, partial, datasetType), record);
    }
    for (const key of partial.deletes ?? []) {
      records.delete(key);
    }
    return [...records.values()];
  }

  private keyOf(
    record: Record<string, unknown>,
    partial: PartialPayload,
    datasetType: string,
  ): string | number {
    const key = partial.keyField
      ? record[partial.keyField]
      : (record.id ?? record.code);
    if (key === undefined || key === null) {
      throw new Error(
        `Partial record for ${datasetType} has no "${partial.keyField ?? 'id/code'}" key to merge on`,
      );
    }
    return key as string | number;
  }

  private verifyHash(message: SyncMessage): void {
    const actual = computeContentHash(message.payload);
    if (actual !== message.contentHash) {
      throw new Error(
        `contentHash mismatch on ${message.datasetType} v${message.version}: message=${message.contentHash.slice(0, 12)} computed=${actual.slice(0, 12)}`,
      );
    }
  }

  async deleteDataset(datasetType: string): Promise<void> {
    await rm(this.fileFor(datasetType), { force: true });
    await rm(this.wireFileFor(datasetType), { force: true });
  }

  async deleteAllDatasets(): Promise<void> {
    await rm(this.datasetsDir, { recursive: true, force: true });
    await rm(this.wireMessagesDir, { recursive: true, force: true });
  }

  private fileFor(datasetType: string): string {
    return join(
      this.datasetsDir,
      `${datasetType.replace(/[^A-Za-z0-9_-]/g, '_')}.json`,
    );
  }

  private wireFileFor(datasetType: string): string {
    return join(
      this.wireMessagesDir,
      `${datasetType.replace(/[^A-Za-z0-9_-]/g, '_')}.json`,
    );
  }

  private async read(datasetType: string): Promise<StoredDataset | null> {
    try {
      return JSON.parse(await readFile(this.fileFor(datasetType), 'utf8'));
    } catch {
      return null;
    }
  }

  private async write(message: SyncMessage, records: unknown): Promise<void> {
    const file = this.fileFor(message.datasetType);
    const stored: StoredDataset = {
      datasetType: message.datasetType,
      version: message.version,
      contentHash: message.contentHash,
      appliedAt: new Date().toISOString(),
      records,
    };
    await mkdir(dirname(file), { recursive: true });
    const tmp = `${file}.tmp`;
    await writeFile(tmp, JSON.stringify(stored, null, 2));
    await rename(tmp, file);
  }

  /** Writes the SyncMessage exactly as received — no added/renamed/dropped fields. */
  private async writeWireMessage(message: SyncMessage): Promise<void> {
    const file = this.wireFileFor(message.datasetType);
    await mkdir(dirname(file), { recursive: true });
    const tmp = `${file}.tmp`;
    await writeFile(tmp, JSON.stringify(message, null, 2));
    await rename(tmp, file);
  }
}
