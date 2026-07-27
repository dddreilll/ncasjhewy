import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Post,
  Query,
} from '@nestjs/common';
import { FleetService, StoreStatus } from './fleet.service';

/**
 * Fleet management API backing the dashboard UI.
 *
 *   GET    /api/fleet                                  fleet status
 *   POST   /api/fleet/stores { storeCodes }            add stores (string or string[])
 *   DELETE /api/fleet/stores/:code?purge=true          stop (purge → delete queue too)
 *   GET    /api/fleet/stores/:code/datasets/:type      inspect an applied dataset
 */
@Controller('api/fleet')
export class FleetController {
  constructor(private readonly fleet: FleetService) {}

  @Get()
  async status(): Promise<{ stores: StoreStatus[] }> {
    return { stores: await this.fleet.fleetStatus() };
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

  @Delete('stores/:code')
  async remove(
    @Param('code') code: string,
    @Query('purge') purge?: string,
  ): Promise<{ removed: string; purged: boolean }> {
    const purged = purge === 'true' || purge === '1';
    await this.fleet.removeStore(code, purged);
    return { removed: code, purged };
  }

  @Get('stores/:code/datasets/:type')
  async dataset(
    @Param('code') code: string,
    @Param('type') type: string,
  ): Promise<unknown> {
    return this.fleet.readDataset(code, type);
  }
}
