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
import { Reconciler } from './reconciler';

export interface EdgeConsumerOptions {
  exchange: string;
  dlx: string;
  prefetch: number;
}

export interface EdgeEvent {
  at: string;
  type: 'applied' | 'skipped' | 'gap' | 'failed' | 'reconciled' | 'dropped' | 'wiped';
  message: string;
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
  private readonly reconciling = new Set<string>();
  private readonly events: EdgeEvent[] = [];
  private running = false;

  constructor(
    private readonly storeCode: string,
    private readonly connection: AmqpConnectionManager,
    private readonly options: EdgeConsumerOptions,
    private readonly appliedStore: AppliedVersionStore,
    private readonly applier: DatasetApplier,
    private readonly reconciler: Reconciler,
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

  get recentEvents(): EdgeEvent[] {
    return [...this.events];
  }

  async appliedVersions() {
    return this.appliedStore.all();
  }

  private record(type: EdgeEvent['type'], message: string): void {
    this.events.unshift({ at: new Date().toISOString(), type, message });
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
   * matching message (redelivery, rebroadcast, or reconcile pull) re-applies
   * it from scratch as if this store had never seen it.
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
        );
        this.channelWrapper.ack(msg);
        return;
      }

      if (message.mode === SYNC_MODES.SNAPSHOT) {
        await this.applier.applySnapshot(message);
      } else if (message.previousVersion === applied.version) {
        await this.applier.applyPartial(message);
      } else {
        // Gap: partial lands on a stale base — never apply. Park it and reconcile.
        this.logger.warn(
          `Gap on ${message.datasetType}: partial expects v${message.previousVersion} but at v${applied.version} — reconciling`,
        );
        this.record(
          'gap',
          `${message.datasetType} partial expects v${message.previousVersion}, at v${applied.version}`,
        );
        this.channelWrapper.nack(msg, false, false);
        await this.reconcile(message);
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
      );
      this.channelWrapper.nack(msg, false, false);
      await this.publishAck(message, SYNC_ACK_STATUSES.FAILED, reason);
    }
  }

  /**
   * Close a version gap by pulling the current snapshot from head office and
   * applying it wholesale. Guarded per dataset so a burst of gapped partials
   * triggers one pull, not one per message.
   */
  private async reconcile(gapped: SyncMessage): Promise<void> {
    if (this.reconciling.has(gapped.datasetType)) return;
    this.reconciling.add(gapped.datasetType);

    try {
      const snapshot = await this.reconciler.fetchSnapshot(
        gapped.datasetType,
        gapped.scope,
        gapped.scopeId,
      );
      if (!snapshot) return;

      const applied = await this.appliedStore.get(snapshot.datasetType);
      if (snapshot.version <= applied.version) {
        this.logger.log(
          `Reconcile pull returned v${snapshot.version}, already at v${applied.version} — nothing to do`,
        );
        return;
      }

      await this.applier.applySnapshot(snapshot);
      await this.appliedStore.set(
        snapshot.datasetType,
        snapshot.version,
        snapshot.contentHash,
      );
      await this.publishAck(snapshot, SYNC_ACK_STATUSES.APPLIED);
      this.logger.log(
        `Reconciled ${snapshot.datasetType} to v${snapshot.version} via snapshot pull`,
      );
      this.record(
        'reconciled',
        `${snapshot.datasetType} to v${snapshot.version} via snapshot pull`,
      );
    } catch (error) {
      this.logger.error(
        `Reconcile of ${gapped.datasetType} failed: ${error instanceof Error ? error.message : String(error)}`,
      );
    } finally {
      this.reconciling.delete(gapped.datasetType);
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
  }
}
