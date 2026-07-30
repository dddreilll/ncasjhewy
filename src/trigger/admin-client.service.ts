import {
  Injectable,
  Logger,
  ServiceUnavailableException,
} from '@nestjs/common';
import { AppConfigService } from '../config/app-config.service';

export interface TriggerResult {
  status: number;
  body: unknown;
}

interface Session {
  accessToken: string;
  refreshToken?: string;
  /** epoch ms */
  expiresAt: number;
  /** epoch ms */
  refreshExpiresAt?: number;
}

interface AccessTokenResponse {
  accessToken: string;
  refreshToken?: string;
  expiresIn: number;
  refreshTokenExpiresIn?: number;
  message?: string | string[];
}

/** Refresh/re-sign-in this far ahead of actual expiry, so a token never dies mid-request. */
const SAFETY_MARGIN_MS = 30_000;

/**
 * Holds one authenticated session against app-gateway's real /auth endpoints
 * (the same sign-in flow any CDH user goes through) so the dashboard can call
 * the guarded POST /store-data-sync/<dataset> endpoints without a person
 * re-entering credentials per click. Signs in once, then reuses/refreshes the
 * session — never re-signs-in on every trigger, since repeated sign-ins churn
 * session rows and can trip head office's login-attempt lockout.
 */
@Injectable()
export class AdminClient {
  private readonly logger = new Logger(AdminClient.name);
  private session?: Session;
  private signingIn?: Promise<Session>;

  constructor(private readonly config: AppConfigService) {}

  get isConfigured(): boolean {
    return Boolean(
      this.config.getGatewayAdminBaseUrl() &&
      this.config.getGatewayAdminUsername() &&
      this.config.getGatewayAdminPassword(),
    );
  }

  /** POST /store-data-sync/<dataset> with a valid bearer token, retrying once on 401. */
  async trigger(
    dataset: string,
    body: Record<string, unknown>,
  ): Promise<TriggerResult> {
    if (!this.isConfigured) {
      throw new ServiceUnavailableException(
        'Admin trigger is not configured — set GATEWAY_ADMIN_BASE_URL, GATEWAY_ADMIN_USERNAME, and GATEWAY_ADMIN_PASSWORD',
      );
    }

    const token = await this.getToken();
    const first = await this.postSync(dataset, body, token.accessToken);
    if (first.status !== 401) return first;

    this.logger.warn(
      'Trigger got 401 with a cached token — signing in fresh and retrying once',
    );
    this.session = undefined;
    const fresh = await this.getToken();
    return this.postSync(dataset, body, fresh.accessToken);
  }

  private async postSync(
    dataset: string,
    body: Record<string, unknown>,
    accessToken: string,
  ): Promise<TriggerResult> {
    const url = new URL(
      `/api/v1/store-data-sync/${dataset}`,
      this.config.getGatewayAdminBaseUrl(),
    );
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
      body: JSON.stringify(body),
    });
    const parsed = await res.json().catch(() => ({}));
    return { status: res.status, body: parsed };
  }

  private async getToken(): Promise<Session> {
    const now = Date.now();

    if (this.session && this.session.expiresAt > now + SAFETY_MARGIN_MS) {
      return this.session;
    }
    if (
      this.session?.refreshToken &&
      (this.session.refreshExpiresAt ?? 0) > now + SAFETY_MARGIN_MS
    ) {
      try {
        return await this.refresh(this.session.refreshToken);
      } catch (error) {
        this.logger.warn(
          `Refresh failed, signing in fresh: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }
    return this.signIn();
  }

  /** De-dupes concurrent sign-in attempts (e.g. two trigger calls racing on a cold session) into one. */
  private signIn(): Promise<Session> {
    if (!this.signingIn) {
      this.signingIn = this.doSignIn().finally(() => {
        this.signingIn = undefined;
      });
    }
    return this.signingIn;
  }

  private async doSignIn(): Promise<Session> {
    const url = new URL(
      '/api/v1/auth/sign-in',
      this.config.getGatewayAdminBaseUrl(),
    );
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        username: this.config.getGatewayAdminUsername(),
        password: this.config.getGatewayAdminPassword(),
        deviceName: this.config.getGatewayAdminDeviceName(),
      }),
    });
    const data = (await res.json().catch(() => ({}))) as AccessTokenResponse;
    if (!res.ok) {
      const message = Array.isArray(data.message)
        ? data.message.join('; ')
        : data.message;
      throw new Error(
        `Admin sign-in failed: HTTP ${res.status}${message ? ` — ${message}` : ''}`,
      );
    }

    this.session = this.toSession(data);
    this.logger.log('Signed in to app-gateway for admin triggers');
    return this.session;
  }

  private async refresh(refreshToken: string): Promise<Session> {
    const url = new URL(
      '/api/v1/auth/refresh',
      this.config.getGatewayAdminBaseUrl(),
    );
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refreshToken }),
    });
    const data = (await res.json().catch(() => ({}))) as AccessTokenResponse;
    if (!res.ok) {
      throw new Error(`Admin token refresh failed: HTTP ${res.status}`);
    }

    this.session = this.toSession(data);
    return this.session;
  }

  private toSession(data: AccessTokenResponse): Session {
    const now = Date.now();
    return {
      accessToken: data.accessToken,
      refreshToken: data.refreshToken,
      expiresAt: now + Number(data.expiresIn ?? 0) * 1000,
      refreshExpiresAt: data.refreshTokenExpiresIn
        ? now + Number(data.refreshTokenExpiresIn) * 1000
        : undefined,
    };
  }
}
