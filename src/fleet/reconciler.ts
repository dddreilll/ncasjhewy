import { Logger } from '@nestjs/common';
import { DatasetScope, SyncMessage } from '../contracts/data-sync';

/**
 * Pull path for gap recovery and cold starts: fetch the current snapshot
 * message for a dataset from head office over HTTP. Until the snapshot
 * endpoint exists on the gateway (RECONCILE_BASE_URL unset), this stays a
 * logged hook — mirroring the reference edge in fusion-cdh-api.
 */
export class Reconciler {
  constructor(
    private readonly baseUrl: string | undefined,
    private readonly snapshotPath: string,
    private readonly logger: Logger,
  ) {}

  async fetchSnapshot(
    datasetType: string,
    scope: DatasetScope,
    scopeId: string | null,
  ): Promise<SyncMessage | null> {
    if (!this.baseUrl) {
      this.logger.warn(
        `Reconciliation required for ${datasetType} but RECONCILE_BASE_URL is not set — pull the latest snapshot from head office manually`,
      );
      return null;
    }

    const url = new URL(this.snapshotPath, this.baseUrl);
    url.searchParams.set('datasetType', datasetType);
    url.searchParams.set('scope', scope);
    if (scopeId) url.searchParams.set('scopeId', scopeId);

    try {
      const response = await fetch(url, {
        headers: { accept: 'application/json' },
      });
      if (!response.ok) {
        this.logger.error(
          `Snapshot pull for ${datasetType} failed: HTTP ${response.status}`,
        );
        return null;
      }

      const body = (await response.json()) as
        | SyncMessage
        | { data?: SyncMessage };
      // tolerate the gateway's { data: ... } response wrapper
      const message =
        'datasetType' in body ? body : ((body.data ?? null) as SyncMessage | null);
      if (!message?.datasetType) {
        this.logger.error(
          `Snapshot pull for ${datasetType} returned no message`,
        );
        return null;
      }
      return message;
    } catch (error) {
      this.logger.error(
        `Snapshot pull for ${datasetType} failed: ${error instanceof Error ? error.message : String(error)}`,
      );
      return null;
    }
  }
}
