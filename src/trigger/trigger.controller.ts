import {
  BadRequestException,
  Body,
  Controller,
  Get,
  HttpException,
  Param,
  Post,
} from '@nestjs/common';
import { AdminClient } from './admin-client.service';

/** Must match app-gateway's POST /store-data-sync/<dataset> routes. */
const STORE_SCOPED_DATASETS = new Set(['employees', 'menu', 'store']);
const GLOBAL_DATASETS = new Set([
  'roles',
  'payment-types',
  'channels',
  'cash-denominations',
]);

/**
 * Proxies to app-gateway's guarded POST /store-data-sync/<dataset> endpoints
 * so every test scenario (including preview/isTest) can be driven from this
 * dashboard instead of needing a separate Swagger tab. Fully optional — see
 * AdminClient.isConfigured; the dashboard hides the trigger panel when unset.
 */
@Controller('api/trigger')
export class TriggerController {
  constructor(private readonly admin: AdminClient) {}

  @Get('status')
  status(): { configured: boolean } {
    return { configured: this.admin.isConfigured };
  }

  @Post(':dataset')
  async trigger(
    @Param('dataset') dataset: string,
    @Body('storeCode') storeCode?: string,
    @Body('isTest') isTest?: boolean,
  ): Promise<{
    dataset: string;
    requestBody: Record<string, unknown>;
    response: unknown;
  }> {
    if (!STORE_SCOPED_DATASETS.has(dataset) && !GLOBAL_DATASETS.has(dataset)) {
      throw new BadRequestException(
        `Unknown dataset "${dataset}" — expected one of: ${[...STORE_SCOPED_DATASETS, ...GLOBAL_DATASETS].join(', ')}`,
      );
    }
    if (STORE_SCOPED_DATASETS.has(dataset) && !storeCode?.trim()) {
      throw new BadRequestException(`storeCode is required for "${dataset}"`);
    }

    // GLOBAL datasets' gateway DTOs don't declare a storeCode field and the
    // gateway rejects unknown properties (forbidNonWhitelisted) — omit it.
    const body: Record<string, unknown> = { isTest: Boolean(isTest) };
    if (STORE_SCOPED_DATASETS.has(dataset)) body.storeCode = storeCode!.trim();

    const { status, body: response } = await this.admin.trigger(dataset, body);
    if (status < 200 || status >= 300) {
      const message = (response as { message?: string | string[] } | undefined)
        ?.message;
      const text = Array.isArray(message)
        ? message.join('; ')
        : (message ?? `Gateway returned HTTP ${status}`);
      throw new HttpException(
        text,
        status >= 400 && status < 600 ? status : 502,
      );
    }

    return { dataset, requestBody: body, response };
  }
}
