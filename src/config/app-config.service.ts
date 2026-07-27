import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { SYNC_BROKER_DEFAULTS } from '../contracts/data-sync';

/**
 * Store codes become AMQP queue names and routing-key segments, so they must
 * not contain topic wildcards or separators.
 */
const VALID_STORE_CODE = /^[A-Za-z0-9_-]+$/;

export function isValidStoreCode(code: string): boolean {
  return VALID_STORE_CODE.test(code);
}

@Injectable()
export class AppConfigService {
  constructor(private readonly configService: ConfigService) {}

  /**
   * Store codes seeded from the environment — one edge consumer (queue +
   * bindings + local state) is spun up per code, always re-joining on boot.
   * Optional: the fleet can also be grown entirely at runtime via the UI/API
   * (persisted to fleet.json).
   */
  getStoreCodes(): string[] {
    const raw = this.configService.get<string>('STORE_CODES') ?? '';
    const codes = [
      ...new Set(
        raw
          .split(',')
          .map((code) => code.trim())
          .filter(Boolean),
      ),
    ];

    const invalid = codes.filter((code) => !isValidStoreCode(code));
    if (invalid.length > 0) {
      throw new Error(
        `Invalid store code(s) ${invalid.join(', ')} — only letters, digits, "-" and "_" are allowed (codes are embedded in AMQP routing keys)`,
      );
    }
    return codes;
  }

  getHttpPort(): number {
    return Number(this.configService.get('PORT') ?? 4000);
  }

  getRabbitMqUrl(): string {
    const url = this.configService.get<string>('RABBITMQ_URL');
    if (url) return url;

    const user = this.configService.get('RABBITMQ_USER') ?? 'admin';
    const password = this.configService.get('RABBITMQ_PASSWORD') ?? 'admin123';
    const host = this.configService.get('RABBITMQ_HOST') ?? 'localhost';
    const port = this.configService.get('RABBITMQ_PORT') ?? '5674';
    return `amqp://${user}:${password}@${host}:${port}`;
  }

  getSyncExchange(): string {
    return (
      this.configService.get('RABBITMQ_SYNC_EXCHANGE') ||
      SYNC_BROKER_DEFAULTS.EXCHANGE
    );
  }

  getSyncDlx(): string {
    return (
      this.configService.get('RABBITMQ_SYNC_DLX') || SYNC_BROKER_DEFAULTS.DLX
    );
  }

  getSyncPrefetch(): number {
    return Number(this.configService.get('RABBITMQ_SYNC_PREFETCH') ?? 10);
  }

  /** Root directory for per-store JSON state (applied versions + datasets). */
  getDataDir(): string {
    return this.configService.get('DATA_DIR') ?? './data';
  }

  /** Head-office base URL for the reconcile pull path; unset → log-only hook. */
  getReconcileBaseUrl(): string | undefined {
    return this.configService.get('RECONCILE_BASE_URL') || undefined;
  }

  getReconcileSnapshotPath(): string {
    return (
      this.configService.get('RECONCILE_SNAPSHOT_PATH') ||
      '/api/v1/store-data-sync/snapshot'
    );
  }
}
