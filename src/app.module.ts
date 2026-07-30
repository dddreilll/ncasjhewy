import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { FleetModule } from './fleet/fleet.module';
import { TriggerModule } from './trigger/trigger.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), FleetModule, TriggerModule],
})
export class AppModule {}
