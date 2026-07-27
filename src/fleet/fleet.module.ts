import { Module } from '@nestjs/common';
import { AppConfigService } from '../config/app-config.service';
import { FleetController } from './fleet.controller';
import { FleetService } from './fleet.service';
import { UiController } from './ui.controller';

@Module({
  controllers: [FleetController, UiController],
  providers: [AppConfigService, FleetService],
})
export class FleetModule {}
