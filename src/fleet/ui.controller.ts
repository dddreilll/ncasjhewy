import { Controller, Get, Header } from '@nestjs/common';
import { readFile } from 'fs/promises';
import { join } from 'path';

/**
 * Serves the single-file dashboard, read fresh on every request. It's one
 * small file — the I/O cost is negligible — and reading it live avoids a
 * class of confusion during dev: `nest start --watch`'s asset-copy step
 * updates `dist/ui/index.html` on disk without necessarily restarting the
 * process, so a cache set once at construction time can silently serve a
 * stale page even after the file on disk (and the API) have moved on.
 */
@Controller()
export class UiController {
  private readonly file = join(__dirname, '..', 'ui', 'index.html');

  @Get()
  @Header('Content-Type', 'text/html; charset=utf-8')
  async index(): Promise<string> {
    return readFile(this.file, 'utf8');
  }
}
