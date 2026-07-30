import { Module } from '@nestjs/common';
import { FleetModule } from '../fleet/fleet.module';
import { AdminClient } from './admin-client.service';
import { TriggerController } from './trigger.controller';

@Module({
  imports: [FleetModule],
  controllers: [TriggerController],
  providers: [AdminClient],
})
export class TriggerModule {}
