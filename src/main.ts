import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { AppConfigService } from './config/app-config.service';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();

  const port = app.get(AppConfigService).getHttpPort();
  await app.listen(port);
  new Logger('Bootstrap').log(`Fleet dashboard on http://localhost:${port}`);
}

void bootstrap();
