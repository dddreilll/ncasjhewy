import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Post,
  Query,
} from '@nestjs/common';
import { AppConfigService } from '../config/app-config.service';
import { FleetService, StoreStatus } from './fleet.service';

/**
 * Fleet management API backing the dashboard UI.
 *
 *   GET    /api/fleet                                  fleet status
 *   GET    /api/fleet/meta                              this app's own public base URL (FLEET_API_BASE_URL)
 *   POST   /api/fleet/stores { storeCodes }            add stores (string or string[])
 *   POST   /api/fleet/stores/:code/stop                pause consuming — stays listed as "stopped", queue kept
 *   POST   /api/fleet/stores/:code/start               resume consuming a stopped store
 *   DELETE /api/fleet/stores/:code?delete=true          remove from the fleet entirely (delete → delete queue too)
 *   POST   /api/fleet/stores/:code/purge                empty the queue only — keeps queue, bindings, and consumer
 *   GET    /api/fleet/stores/:code/datasets/:type      inspect an applied dataset
 *   DELETE /api/fleet/stores/:code/datasets/:type      wipe one locally applied dataset (state + file only)
 *   DELETE /api/fleet/stores/:code/datasets            wipe every locally applied dataset for the store
 *   POST   /api/fleet/stores/:code/inject-failure { datasetType, times? }  force the next apply(s) to FAIL
 *   DELETE /api/fleet/stores/:code/inject-failure/:type  clear a pending failure injection
 */
@Controller('api/fleet')
export class FleetController {
  constructor(
    private readonly fleet: FleetService,
    private readonly config: AppConfigService,
  ) {}

  @Get()
  async status(): Promise<{ stores: StoreStatus[] }> {
    return { stores: await this.fleet.fleetStatus() };
  }

  @Get('meta')
  meta(): { baseUrl: string } {
    return { baseUrl: this.config.getFleetApiBaseUrl() };
  }

  @Post('stores')
  async add(
    @Body('storeCodes') storeCodes: string | string[],
  ): Promise<{ added: string[] }> {
    const codes = Array.isArray(storeCodes)
      ? storeCodes
      : String(storeCodes ?? '').split(',');
    return { added: await this.fleet.addStores(codes) };
  }

  @Post('stores/:code/stop')
  async stop(
    @Param('code') code: string,
  ): Promise<{ storeCode: string; running: false }> {
    await this.fleet.stopStore(code);
    return { storeCode: code, running: false };
  }

  @Post('stores/:code/start')
  async start(
    @Param('code') code: string,
  ): Promise<{ storeCode: string; running: true }> {
    await this.fleet.startStore(code);
    return { storeCode: code, running: true };
  }

  @Delete('stores/:code')
  async remove(
    @Param('code') code: string,
    @Query('delete') deleteParam?: string,
  ): Promise<{ removed: string; deleted: boolean }> {
    const deleteQueue = deleteParam === 'true' || deleteParam === '1';
    await this.fleet.removeStore(code, deleteQueue);
    return { removed: code, deleted: deleteQueue };
  }

  @Post('stores/:code/purge')
  async purge(
    @Param('code') code: string,
  ): Promise<{ storeCode: string; messageCount: number }> {
    const { messageCount } = await this.fleet.purgeQueue(code);
    return { storeCode: code, messageCount };
  }

  @Get('stores/:code/datasets/:type')
  async dataset(
    @Param('code') code: string,
    @Param('type') type: string,
  ): Promise<unknown> {
    return this.fleet.readDataset(code, type);
  }

  @Delete('stores/:code/datasets/:type')
  async wipeDataset(
    @Param('code') code: string,
    @Param('type') type: string,
  ): Promise<{ storeCode: string; datasetType: string; wiped: true }> {
    await this.fleet.wipeDataset(code, type);
    return { storeCode: code, datasetType: type, wiped: true };
  }

  @Delete('stores/:code/datasets')
  async wipeAllDatasets(
    @Param('code') code: string,
  ): Promise<{ storeCode: string; wiped: true }> {
    await this.fleet.wipeAllDatasets(code);
    return { storeCode: code, wiped: true };
  }

  @Post('stores/:code/inject-failure')
  async injectFailure(
    @Param('code') code: string,
    @Body('datasetType') datasetType: string,
    @Body('times') times?: number,
  ): Promise<{ storeCode: string; datasetType: string; times: number }> {
    if (!datasetType) {
      throw new BadRequestException('datasetType is required');
    }
    const parsed = Number(times);
    const n = Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 1;
    this.fleet.injectFailure(code, datasetType, n);
    return { storeCode: code, datasetType, times: n };
  }

  @Delete('stores/:code/inject-failure/:type')
  async clearInjectFailure(
    @Param('code') code: string,
    @Param('type') type: string,
  ): Promise<{ storeCode: string; datasetType: string; cleared: true }> {
    this.fleet.clearFailureInjection(code, type);
    return { storeCode: code, datasetType: type, cleared: true };
  }
}
