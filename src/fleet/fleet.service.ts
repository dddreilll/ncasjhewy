import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import {
  AmqpConnectionManager,
  ChannelWrapper,
  connect,
} from 'amqp-connection-manager';
import { readFile } from 'fs/promises';
import { join } from 'path';
import { AppConfigService, isValidStoreCode } from '../config/app-config.service';
import { AppliedVersion, FileAppliedVersionStore } from './applied-version.store';
import { FileDatasetApplier } from './dataset-applier';
import { EdgeConsumer, EdgeEvent } from './edge-consumer';
import { FleetRegistry } from './fleet-registry';
import { Reconciler } from './reconciler';

export interface StoreStatus {
  storeCode: string;
  queue: string;
  running: boolean;
  fromEnv: boolean;
  applied: Record<string, AppliedVersion>;
  queueStats: { messageCount: number; consumerCount: number } | null;
  recentEvents: EdgeEvent[];
}

/**
 * Manages a dynamic fleet of EdgeConsumers over a single shared AMQP
 * connection (channels are cheap; connections are not). Boot fleet = env
 * STORE_CODES ∪ fleet.json registry; stores added/removed at runtime are
 * persisted to the registry so they survive restarts. Env-seeded codes always
 * re-join on boot.
 */
@Injectable()
export class FleetService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(FleetService.name);
  private connection?: AmqpConnectionManager;
  private adminChannel?: ChannelWrapper;
  private readonly consumers = new Map<string, EdgeConsumer>();
  private registry!: FleetRegistry;
  private envCodes: string[] = [];

  constructor(private readonly config: AppConfigService) {}

  async onModuleInit(): Promise<void> {
    this.registry = new FleetRegistry(this.config.getDataDir());
    this.envCodes = this.config.getStoreCodes();

    this.connection = connect([this.config.getRabbitMqUrl()]);
    this.connection.on('connect', () =>
      this.logger.log('Fleet connected to RabbitMQ'),
    );
    this.connection.on('disconnect', ({ err }) =>
      this.logger.warn(`Fleet disconnected: ${err?.message}`),
    );
    this.adminChannel = this.connection.createChannel({ json: false });

    const bootCodes = [
      ...new Set([...this.envCodes, ...(await this.registry.load())]),
    ];
    for (const storeCode of bootCodes) this.startConsumer(storeCode);
    await this.registry.save(bootCodes);

    this.logger.log(
      `Fleet of ${bootCodes.length} store(s) starting: ${bootCodes.join(', ') || '(none — add via UI/API)'}`,
    );
  }

  /** Spin up new virtual stores at runtime. Returns codes actually added. */
  async addStores(storeCodes: string[]): Promise<string[]> {
    const codes = [...new Set(storeCodes.map((code) => code.trim()))].filter(
      Boolean,
    );
    if (codes.length === 0) {
      throw new BadRequestException('No store codes given');
    }
    const invalid = codes.filter((code) => !isValidStoreCode(code));
    if (invalid.length > 0) {
      throw new BadRequestException(
        `Invalid store code(s): ${invalid.join(', ')} — only letters, digits, "-" and "_" are allowed`,
      );
    }

    const added: string[] = [];
    for (const code of codes) {
      if (this.consumers.has(code)) continue;
      this.startConsumer(code);
      added.push(code);
    }
    if (added.length > 0) {
      await this.registry.save([...this.consumers.keys()]);
      this.logger.log(`Added store(s): ${added.join(', ')}`);
    }
    return added;
  }

  /**
   * Stop a store. purge=false parks it (durable queue keeps accumulating so it
   * can catch up later); purge=true decommissions it — deletes the broker
   * queue. Local data/<code> files are kept either way for post-mortem.
   */
  async removeStore(storeCode: string, purge: boolean): Promise<void> {
    const consumer = this.consumers.get(storeCode);
    if (!consumer) {
      throw new NotFoundException(`Store ${storeCode} is not in the fleet`);
    }

    await consumer.stop();
    this.consumers.delete(storeCode);
    await this.registry.save([...this.consumers.keys()]);

    if (purge) {
      await this.adminChannel?.deleteQueue(consumer.queueName);
      this.logger.log(`Removed ${storeCode} and deleted ${consumer.queueName}`);
    } else {
      this.logger.log(
        `Stopped ${storeCode} — queue ${consumer.queueName} kept (offline store)`,
      );
    }
  }

  async fleetStatus(): Promise<StoreStatus[]> {
    return Promise.all(
      [...this.consumers.values()].map(async (consumer) => ({
        storeCode: consumer.code,
        queue: consumer.queueName,
        running: consumer.isRunning,
        fromEnv: this.envCodes.includes(consumer.code),
        applied: await consumer.appliedVersions(),
        queueStats: await this.queueStats(consumer.queueName),
        recentEvents: consumer.recentEvents,
      })),
    );
  }

  /** Raw stored dataset file for inspection in the UI. */
  async readDataset(storeCode: string, datasetType: string): Promise<unknown> {
    if (!this.consumers.has(storeCode)) {
      throw new NotFoundException(`Store ${storeCode} is not in the fleet`);
    }
    const file = join(
      this.config.getDataDir(),
      storeCode,
      'datasets',
      `${datasetType.replace(/[^A-Za-z0-9_-]/g, '_')}.json`,
    );
    try {
      return JSON.parse(await readFile(file, 'utf8'));
    } catch {
      throw new NotFoundException(
        `No applied ${datasetType} dataset for ${storeCode}`,
      );
    }
  }

  private startConsumer(storeCode: string): void {
    const dataDir = this.config.getDataDir();
    const edgeLogger = new Logger(`Edge:${storeCode}`);
    const consumer = new EdgeConsumer(
      storeCode,
      this.connection!,
      {
        exchange: this.config.getSyncExchange(),
        dlx: this.config.getSyncDlx(),
        prefetch: this.config.getSyncPrefetch(),
      },
      FileAppliedVersionStore.forStore(dataDir, storeCode),
      FileDatasetApplier.forStore(dataDir, storeCode, edgeLogger),
      new Reconciler(
        this.config.getReconcileBaseUrl(),
        this.config.getReconcileSnapshotPath(),
        edgeLogger,
      ),
    );
    consumer.start();
    this.consumers.set(storeCode, consumer);
  }

  // checkQueue on a missing queue faults the channel; amqp-connection-manager
  // re-opens it, so a null here is cosmetic (stats show as unknown once).
  private async queueStats(
    queue: string,
  ): Promise<{ messageCount: number; consumerCount: number } | null> {
    try {
      const reply = await this.adminChannel?.checkQueue(queue);
      return reply
        ? { messageCount: reply.messageCount, consumerCount: reply.consumerCount }
        : null;
    } catch {
      return null;
    }
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.allSettled(
      [...this.consumers.values()].map((consumer) => consumer.stop()),
    );
    await this.adminChannel?.close();
    await this.connection?.close();
  }
}
