import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { FleetModule } from './fleet/fleet.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), FleetModule],
})
export class AppModule {}
