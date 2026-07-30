import { Logger } from '@nestjs/common';
import { AmqpConnectionManager, ChannelWrapper } from 'amqp-connection-manager';
import { ConfirmChannel, ConsumeMessage } from 'amqplib';
import {
  buildAckRoutingKey,
  buildStoreBindingKey,
  buildStoreQueueName,
  GLOBAL_BINDING_KEY,
  SYNC_ACK_STATUSES,
  SYNC_MODES,
  SYNC_SCHEMA_VERSION,
  SyncAck,
  SyncAckStatus,
  SyncMessage,
} from '../contracts/data-sync';
import { AppliedVersionStore } from './applied-version.store';
import { DatasetApplier } from './dataset-applier';

export interface EdgeConsumerOptions {
  exchange: string;
  dlx: string;
  prefetch: number;
}

export interface EdgeEvent {
  id: number;
  at: string;
  type:
    | 'applied'
    | 'skipped'
    | 'gap'
    | 'failed'
    | 'dropped'
    | 'wiped'
    | 'armed'
    | 'ack';
  message: string;
  /** True when this event carries a raw payload fetchable via getEventRawMessage() — the received SyncMessage for most types, or the outbound SyncAck for 'ack'. */
  hasRawMessage: boolean;
}

/** Internal-only: EdgeEvent plus the raw payload behind it, if any — never serialized as-is (see recentEvents). */
interface StoredEvent extends EdgeEvent {
  rawMessage?: SyncMessage | SyncAck;
}

const MAX_EVENTS = 50;

/**
 * One virtual store. Declares the store's durable queue bound to BOTH the
 * global pattern and its own store pattern, applies each message with an
 * idempotent version guard, and acknowledges back to head office. Instances
 * share the process's AMQP connection but own their channel, queue, and local
 * state — so N instances behave exactly like N independent store edges.
 */
export class EdgeConsumer {
  private readonly logger: Logger;
  private channelWrapper?: ChannelWrapper;
  private readonly events: StoredEvent[] = [];
  private nextEventId = 1;
  private running = false;
  /** datasetType -> remaining applies to force-fail, for FAILED-ack testing. */
  private readonly failureInjections = new Map<string, number>();

  constructor(
    private readonly storeCode: string,
    private readonly connection: AmqpConnectionManager,
    private readonly options: EdgeConsumerOptions,
    private readonly appliedStore: AppliedVersionStore,
    private readonly applier: DatasetApplier,
  ) {
    this.logger = new Logger(`Edge:${storeCode}`);
  }

  get code(): string {
    return this.storeCode;
  }

  get queueName(): string {
    return buildStoreQueueName(this.storeCode);
  }

  get isRunning(): boolean {
    return this.running;
  }

  /** Public event feed — strips the raw message payload to keep GET /api/fleet polling cheap; fetch it on demand via getEventRawMessage(). */
  get recentEvents(): EdgeEvent[] {
    return this.events.map(({ rawMessage: _rawMessage, ...event }) => event);
  }

  get pendingFailureInjections(): Record<string, number> {
    return Object.fromEntries(this.failureInjections);
  }

  async appliedVersions() {
    return this.appliedStore.all();
  }

  /** The raw payload behind a given event, if any and if it hasn't aged out of the MAX_EVENTS ring buffer. */
  getEventRawMessage(eventId: number): SyncMessage | SyncAck | undefined {
    return this.events.find((event) => event.id === eventId)?.rawMessage;
  }

  private record(
    type: EdgeEvent['type'],
    message: string,
    rawMessage?: SyncMessage | SyncAck,
  ): void {
    this.events.unshift({
      id: this.nextEventId++,
      at: new Date().toISOString(),
      type,
      message,
      hasRawMessage: rawMessage !== undefined,
      rawMessage,
    });
    if (this.events.length > MAX_EVENTS) this.events.pop();
  }

  start(): void {
    const { exchange, dlx, prefetch } = this.options;
    const queue = buildStoreQueueName(this.storeCode);
    this.running = true;

    this.channelWrapper = this.connection.createChannel({
      json: false,
      setup: async (channel: ConfirmChannel) => {
        await channel.assertExchange(exchange, 'topic', { durable: true });
        await channel.assertExchange(dlx, 'topic', { durable: true });
        await channel.assertQueue(queue, {
          durable: true,
          arguments: { 'x-dead-letter-exchange': dlx },
        });
        await channel.bindQueue(queue, exchange, GLOBAL_BINDING_KEY);
        await channel.bindQueue(
          queue,
          exchange,
          buildStoreBindingKey(this.storeCode),
        );
        await channel.prefetch(prefetch);
        await channel.consume(queue, (msg) => void this.handleMessage(msg), {
          noAck: false,
        });
        this.logger.log(
          `Consuming ${queue} (bindings: ${GLOBAL_BINDING_KEY}, ${buildStoreBindingKey(this.storeCode)})`,
        );
      },
    });
    this.channelWrapper.on('error', (err) =>
      this.logger.error(`Channel error: ${err?.message}`),
    );
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.channelWrapper?.close();
  }

  /**
   * Delete the locally applied copy of one dataset (state + file) without
   * touching the broker queue or consumer — purely local, so the next
   * matching message (redelivery or rebroadcast) re-applies it from scratch
   * as if this store had never seen it.
   */
  async wipeDataset(datasetType: string): Promise<void> {
    await this.appliedStore.clear(datasetType);
    await this.applier.deleteDataset(datasetType);
    this.logger.warn(`Wiped local dataset ${datasetType} (state + file)`);
    this.record('wiped', `${datasetType} wiped`);
  }

  /** Same as wipeDataset() but for every dataset this store has applied. */
  async wipeAllDatasets(): Promise<void> {
    await this.appliedStore.clearAll();
    await this.applier.deleteAllDatasets();
    this.logger.warn('Wiped ALL locally applied datasets (state + files)');
    this.record('wiped', 'All datasets wiped');
  }

  /**
   * Arm the next `times` apply attempt(s) of `datasetType` to fail — for
   * testing that head office correctly records and surfaces a FAILED ack.
   * Works whether or not the dataset has ever been applied yet: arming a type
   * with no local state forces the store's very first apply of it to fail.
   */
  injectFailure(datasetType: string, times: number): void {
    const n = Math.max(1, Math.floor(times));
    this.failureInjections.set(datasetType, n);
    this.logger.warn(`Armed ${n} injected failure(s) for ${datasetType}`);
    this.record(
      'armed',
      `${datasetType}: next ${n} apply(s) will be forced to fail`,
    );
  }

  clearFailureInjection(datasetType: string): void {
    if (!this.failureInjections.delete(datasetType)) return;
    this.logger.log(`Cleared injected failure for ${datasetType}`);
    this.record('armed', `${datasetType}: injected failure cleared`);
  }

  /** Consumes one armed failure for `datasetType`, if any. Throws to trigger the normal FAILED-ack path. */
  private maybeInjectFailure(datasetType: string): void {
    const remaining = this.failureInjections.get(datasetType);
    if (!remaining) return;
    if (remaining <= 1) this.failureInjections.delete(datasetType);
    else this.failureInjections.set(datasetType, remaining - 1);
    throw new Error(`Injected failure for ${datasetType} (testing)`);
  }

  private async handleMessage(msg: ConsumeMessage | null): Promise<void> {
    if (!msg || !this.channelWrapper) return;

    let message: SyncMessage;
    try {
      message = JSON.parse(msg.content.toString()) as SyncMessage;
    } catch (error) {
      this.logger.error(
        `Dropping unparseable message: ${error instanceof Error ? error.message : String(error)}`,
      );
      this.record('dropped', 'Unparseable message dead-lettered');
      this.channelWrapper.nack(msg, false, false);
      return;
    }

    if (message.schemaVersion > SYNC_SCHEMA_VERSION) {
      this.logger.warn(
        `Message schema v${message.schemaVersion} is newer than supported v${SYNC_SCHEMA_VERSION} — applying anyway, consider updating the consumer`,
      );
    }

    const applied = await this.appliedStore.get(message.datasetType);

    try {
      // Already applied (or a redelivery of an older version) — idempotent skip.
      if (message.version <= applied.version) {
        this.logger.log(
          `Skip ${message.datasetType} v${message.version} (already at v${applied.version})`,
        );
        this.record(
          'skipped',
          `${message.datasetType} v${message.version} (already at v${applied.version})`,
          message,
        );
        this.channelWrapper.ack(msg);
        return;
      }

      this.maybeInjectFailure(message.datasetType);

      if (message.mode === SYNC_MODES.SNAPSHOT) {
        await this.applier.applySnapshot(message);
      } else if (message.previousVersion === applied.version) {
        await this.applier.applyPartial(message);
      } else {
        // Gap: partial lands on a stale base — never apply. Dead-letter it and
        // surface the gap on the dashboard; there is no automatic recovery —
        // a human resolves it by manually triggering a real (non-preview)
        // sync for this dataset via the gateway's Scalar/Swagger API (or this
        // dashboard's Trigger panel, which calls the same endpoint), which
        // always rebroadcasts a full SNAPSHOT and is therefore gap-safe.
        this.logger.warn(
          `Gap on ${message.datasetType}: partial expects v${message.previousVersion} but at v${applied.version} — trigger a manual sync via the gateway API to resolve`,
        );
        this.record(
          'gap',
          `${message.datasetType} partial expects v${message.previousVersion}, at v${applied.version} — trigger a manual sync via the gateway API to resolve`,
          message,
        );
        this.channelWrapper.nack(msg, false, false);
        return;
      }

      await this.appliedStore.set(
        message.datasetType,
        message.version,
        message.contentHash,
      );
      this.channelWrapper.ack(msg);
      this.record(
        'applied',
        `${message.datasetType} v${message.version} (${message.mode})`,
        message,
      );
      await this.publishAck(message, SYNC_ACK_STATUSES.APPLIED);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to apply ${message.datasetType} v${message.version}: ${reason}`,
      );
      this.record(
        'failed',
        `${message.datasetType} v${message.version}: ${reason}`,
        message,
      );
      this.channelWrapper.nack(msg, false, false);
      await this.publishAck(message, SYNC_ACK_STATUSES.FAILED, reason);
    }
  }

  private async publishAck(
    message: SyncMessage,
    status: SyncAckStatus,
    error?: string,
  ): Promise<void> {
    if (!this.channelWrapper) return;

    const ack: SyncAck = {
      storeCode: this.storeCode,
      datasetType: message.datasetType,
      version: message.version,
      status,
      contentHash: message.contentHash,
      ...(error ? { error } : {}),
    };

    await this.channelWrapper.publish(
      this.options.exchange,
      buildAckRoutingKey(message.datasetType, this.storeCode),
      Buffer.from(JSON.stringify(ack)),
      { persistent: true, contentType: 'application/json' },
    );
    this.record(
      'ack',
      `${ack.datasetType} v${ack.version} -> ${ack.status}`,
      ack,
    );
  }
}
