/**
 * VENDORED WIRE CONTRACT — keep in sync with
 * fusion-cdh-api/libs/contracts/src/data-sync/ (constants, routing, message,
 * ack) and DatasetVersioningService.computeContentHash (hashing).
 *
 * The contract between head office and store edges is JSON-over-AMQP plus the
 * routing-key conventions below; this file is the consumer's single source of
 * truth for it. schemaVersion on the message guards structural drift.
 */
import { createHash } from 'crypto';

/**
 * Scope of a dataset.
 * - GLOBAL: one version shared by every store (e.g. payment-types).
 * - STORE:  versioned independently per store, keyed by store code.
 */
export const DATASET_SCOPES = {
  GLOBAL: 'GLOBAL',
  STORE: 'STORE',
} as const;

export type DatasetScope = (typeof DATASET_SCOPES)[keyof typeof DATASET_SCOPES];

/**
 * How a publication's payload should be interpreted by the edge consumer.
 * - SNAPSHOT: payload is the full dataset; apply wholesale.
 * - PARTIAL:    payload is { upserts, deletes }; apply only if previousVersion === applied.
 */
export const SYNC_MODES = {
  SNAPSHOT: 'SNAPSHOT',
  PARTIAL: 'PARTIAL',
} as const;

export type SyncMode = (typeof SYNC_MODES)[keyof typeof SYNC_MODES];

/** Result recorded by an edge after attempting to apply a message. */
export const SYNC_ACK_STATUSES = {
  APPLIED: 'APPLIED',
  FAILED: 'FAILED',
  PENDING: 'PENDING',
} as const;

export type SyncAckStatus =
  (typeof SYNC_ACK_STATUSES)[keyof typeof SYNC_ACK_STATUSES];

/** Current structural version of the message payload contract. */
export const SYNC_SCHEMA_VERSION = 1;

/** Broker topology defaults (must match head office). */
export const SYNC_BROKER_DEFAULTS = {
  EXCHANGE: 'cdh.datasync',
  DLX: 'cdh.datasync.dlx',
} as const;

/** Binding every store queue uses to receive all GLOBAL datasets. */
export const GLOBAL_BINDING_KEY = 'dataset.*.global';

/**
 * The versioned message every store edge receives. `payload` carries the actual
 * dataset (full snapshot, or { upserts, deletes } for a partial). Everything else
 * is the versioning message the edge uses to apply idempotently.
 */
export interface SyncMessage<TPayload = unknown> {
  datasetType: string;
  scope: DatasetScope;
  scopeId: string | null;
  version: number;
  previousVersion: number | null;
  mode: SyncMode;
  contentHash: string;
  schemaVersion: number;
  issuedAt: string;
  issuedBy: string;
  payload: TPayload;
}

/**
 * Partial payload shape (mode === PARTIAL). `recordsField` names the array inside
 * the stored snapshot wrapper to merge into (absent → the snapshot itself is
 * the array); `keyField` names each record's identity field (absent → `id`
 * then `code`).
 */
export interface PartialPayload<TRecord = Record<string, unknown>> {
  recordsField?: string;
  keyField?: string;
  upserts?: TRecord[];
  deletes?: Array<string | number>;
}

/**
 * Sent by a store edge after attempting to apply a message. Head office
 * upserts this into store_sync_state to track per-store sync health.
 */
export interface SyncAck {
  storeCode: string;
  datasetType: string;
  version: number;
  status: SyncAckStatus;
  contentHash?: string;
  error?: string;
}

/** Binding a specific store queue uses to receive its STORE-scoped datasets. */
export function buildStoreBindingKey(storeCode: string): string {
  return `dataset.*.store.${storeCode}`;
}

/** Routing key an edge stamps on an acknowledgement. */
export function buildAckRoutingKey(
  datasetType: string,
  storeCode: string,
): string {
  return `dataset.ack.${datasetType}.${storeCode}`;
}

/** Durable queue name for a given store's edge consumer. */
export function buildStoreQueueName(storeCode: string): string {
  return `q.store.${storeCode}`;
}

/**
 * Deterministic content hash (stable key ordering) so equal data → equal hash.
 * Mirrors DatasetVersioningService.computeContentHash — the edge recomputes it
 * to verify payload integrity before applying.
 */
export function computeContentHash(payload: unknown): string {
  return createHash('sha256').update(stableStringify(payload)).digest('hex');
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value) ?? 'null';
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }
  const record = value as Record<string, unknown>;
  const entries = Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`);
  return `{${entries.join(',')}}`;
}
