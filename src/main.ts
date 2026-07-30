import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { AppConfigService } from './config/app-config.service';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();

  const config = app.get(AppConfigService);
  const baseUrl = config.getFleetApiBaseUrl();
  // CORS matches against the browser's Origin header, which is scheme://host:port
  // only — never a path. FLEET_API_BASE_URL may include a path (e.g. a
  // reverse-proxy subpath like http://host/dev-tools/), so strip it here;
  // the full configured value (path included) is still what /api/fleet/meta
  // returns for display.
  const logger = new Logger('Bootstrap');
  let corsOrigin = baseUrl;
  try {
    corsOrigin = new URL(baseUrl).origin;
  } catch {
    logger.warn(
      `FLEET_API_BASE_URL "${baseUrl}" isn't a valid URL — using it as-is for CORS`,
    );
  }
  app.enableCors({ origin: corsOrigin });

  await app.listen(config.getHttpPort());
  logger.log(`Fleet dashboard on ${baseUrl}`);
}

void bootstrap();
