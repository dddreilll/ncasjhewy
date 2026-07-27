import { mkdir, readFile, rename, writeFile } from 'fs/promises';
import { dirname, join } from 'path';

/**
 * Persists the desired fleet (`<dataDir>/fleet.json`) so stores added at
 * runtime survive restarts. STORE_CODES from the environment seeds the fleet
 * and always re-joins on boot; codes added via the API/UI live here.
 */
export class FleetRegistry {
  private readonly file: string;

  constructor(dataDir: string) {
    this.file = join(dataDir, 'fleet.json');
  }

  async load(): Promise<string[]> {
    try {
      const parsed = JSON.parse(await readFile(this.file, 'utf8'));
      return Array.isArray(parsed?.storeCodes) ? parsed.storeCodes : [];
    } catch {
      return [];
    }
  }

  async save(storeCodes: string[]): Promise<void> {
    await mkdir(dirname(this.file), { recursive: true });
    const tmp = `${this.file}.tmp`;
    await writeFile(
      tmp,
      JSON.stringify({ storeCodes: [...storeCodes].sort() }, null, 2),
    );
    await rename(tmp, this.file);
  }
}
