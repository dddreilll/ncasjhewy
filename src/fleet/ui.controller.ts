import { Controller, Get, Header } from '@nestjs/common';
import { readFileSync } from 'fs';
import { join } from 'path';

/**
 * Serves the single-file dashboard. The HTML is copied to dist/ui by the
 * nest-cli assets config; read once at startup.
 */
@Controller()
export class UiController {
  private readonly html = readFileSync(
    join(__dirname, '..', 'ui', 'index.html'),
    'utf8',
  );

  @Get()
  @Header('Content-Type', 'text/html; charset=utf-8')
  index(): string {
    return this.html;
  }
}
