import { mkdir, readFile, rename, writeFile } from 'fs/promises';
import { dirname, join } from 'path';

export interface AppliedVersion {
  version: number;
  contentHash?: string;
  appliedAt?: string;
}

export interface AppliedVersionStore {
  get(datasetType: string): Promise<AppliedVersion>;
  set(datasetType: string, version: number, contentHash: string): Promise<void>;
  all(): Promise<Record<string, AppliedVersion>>;
}

/**
 * Persists the last applied version per dataset for ONE store as a JSON file
 * (`<dataDir>/<storeCode>/state.json`), so the idempotency guard survives
 * restarts and each virtual store's state stays isolated and inspectable.
 * A real POS would swap this for its local database.
 */
export class FileAppliedVersionStore implements AppliedVersionStore {
  private cache: Record<string, AppliedVersion> | null = null;

  constructor(private readonly stateFile: string) {}

  static forStore(dataDir: string, storeCode: string): FileAppliedVersionStore {
    return new FileAppliedVersionStore(join(dataDir, storeCode, 'state.json'));
  }

  async get(datasetType: string): Promise<AppliedVersion> {
    const state = await this.load();
    return state[datasetType] ?? { version: 0 };
  }

  async all(): Promise<Record<string, AppliedVersion>> {
    return { ...(await this.load()) };
  }

  async set(
    datasetType: string,
    version: number,
    contentHash: string,
  ): Promise<void> {
    const state = await this.load();
    state[datasetType] = {
      version,
      contentHash,
      appliedAt: new Date().toISOString(),
    };
    await this.persist(state);
  }

  private async load(): Promise<Record<string, AppliedVersion>> {
    if (this.cache) return this.cache;
    try {
      this.cache = JSON.parse(await readFile(this.stateFile, 'utf8'));
    } catch {
      this.cache = {};
    }
    return this.cache!;
  }

  // tmp-write + rename so a crash mid-write never truncates the state file
  private async persist(state: Record<string, AppliedVersion>): Promise<void> {
    await mkdir(dirname(this.stateFile), { recursive: true });
    const tmp = `${this.stateFile}.tmp`;
    await writeFile(tmp, JSON.stringify(state, null, 2));
    await rename(tmp, this.stateFile);
  }
}
