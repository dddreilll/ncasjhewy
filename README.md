# fusion-cdh-store-consumer

External store-edge consumer **tester** for the Fusion CDH data-sync pipeline.
It plays the store side of the versioned dataset sync: consuming messages
published by `fusion-cdh-api`'s `data-sync-service` on the
`cdh.datasync` topic exchange, applying them idempotently, and acknowledging
back to head office.

A "store" here is pure configuration. This process hosts **one edge consumer
per code in `STORE_CODES`** — each with its own durable queue, bindings, and
isolated local state — so a single GLOBAL publish from head office fans out to
every configured store, exactly as it would across a real fleet.

```
data-sync-service              cdh.datasync (topic)
  publish dataset.menu.global  ──────►  ┌─────────────────────┐
                                        │ dataset.*.global ───┼──► q.store.S001 ─► edge S001
                                        │                  ───┼──► q.store.S002 ─► edge S002
                                        │                  ───┼──► q.store.S0NN ─► edge S0NN
  ack-consumer ◄── dataset.ack.menu.* ──┤                     │      (each: JSON state,
  (store_sync_state per store)          └─────────────────────┘       version guard, ack)
```

## Modes

| Mode | STORE_CODES | Shape |
|---|---|---|
| Single edge | `S001` | Behaves like one real store (production shape) |
| Fleet simulator | `S001,S002,...,S050` | N virtual stores in one process — cheap fan-out testing |
| Fleet (containers) | one code per container | `docker-compose.yml` — production-faithful |
| Dynamic | empty / any | Grow the fleet at runtime via the dashboard or API |

## Quick start (host, against local head office)

```bash
cp .env.example .env      # defaults match fusion-cdh-api's docker-compose RabbitMQ (localhost:5674)
npm install
npm run start:dev
```

Open the **fleet dashboard** at <http://localhost:4000>, then trigger a publish
from head office (e.g. `POST /api/v1/store-data-sync/payment-types` on the
app-gateway) and watch each store apply it and ack.

Verify results in four places:

1. **Dashboard** — per-store status, queue depth, applied versions (click a
   dataset chip to inspect the applied payload; click a row for the event feed).
2. **Logs** — `APPLIED SNAPSHOT payment-types v3` per store.
3. **Local state** — `data/<storeCode>/state.json` (applied versions) and
   `data/<storeCode>/datasets/<type>.json` (the applied payload).
4. **Head office** — `store_sync_state` table in the data-sync-service
   DB gains one row per store code per dataset.

## Dashboard & fleet API

The dashboard (and its API) manage the fleet **dynamically** — stores added at
runtime are persisted to `data/fleet.json` and rejoin on restart. Env
`STORE_CODES` seeds the fleet and always rejoins on boot.

| Endpoint | Purpose |
|---|---|
| `GET /` | Dashboard UI |
| `GET /api/fleet` | Fleet status: per store — running, queue depth/consumers, applied versions, recent events |
| `POST /api/fleet/stores` `{"storeCodes":"S004,S005"}` | Spin up new virtual stores on the fly |
| `DELETE /api/fleet/stores/:code` | Stop a store, **keep** its queue (offline store — catches up on restart) |
| `DELETE /api/fleet/stores/:code?purge=true` | Decommission: stop + delete the broker queue |
| `GET /api/fleet/stores/:code/datasets/:type` | Inspect a store's applied dataset |

Adding a store declares its durable queue immediately, so it receives
everything published from that moment; history before that needs the reconcile
pull (below) or a fresh publish. Local `data/<code>/` files are kept on remove
for post-mortem — delete manually if unwanted.

## Fleet mode (containers)

Requires the `fusion-cdh-api` compose stack running (provides RabbitMQ and the
`fusion-cdh-api_foodgroup_network` network):

```bash
docker compose up --build
```

Each container serves its own dashboard (S001 → `:4001`, S002 → `:4002`,
S003 → `:4003`). Onboarding another store = add a service entry with a new
`STORE_CODES` value (or a code to the simulator list) and restart — or add it
live through any container's dashboard. Its queue is declared on first
connect; anything published **before** that is recovered via the reconcile pull
(see below) once the endpoint exists — until then, trigger a fresh publish.

## How applying works

- **Version guard** — `message.version <= applied.version` is acked and
  skipped (idempotent replays).
- **SNAPSHOT** — payload verified against `contentHash` (same stable-stringify
  SHA-256 as head office) and written wholesale.
- **PARTIAL** — applied only when `previousVersion` matches the applied version;
  `{ upserts, deletes }` merged into the stored snapshot's `recordsField` array,
  matching records by `keyField`.
- **Gap** (partial on a stale base) — never applied: the message is dead-lettered
  and a snapshot pull is attempted.
- **Failure** — message dead-lettered to `cdh.datasync.dlx`, `FAILED` ack sent
  with the error, visible in head office's `store_sync_state`.

## Reconcile pull (gap / cold-start recovery)

`RECONCILE_BASE_URL` unset (default): gaps are logged only — mirroring the
reference edge in `fusion-cdh-api`. Once the gateway exposes a snapshot
endpoint, set:

```bash
RECONCILE_BASE_URL=http://localhost:1000
RECONCILE_SNAPSHOT_PATH=/api/v1/store-data-sync/snapshot   # default
```

The consumer then closes gaps itself: pull current snapshot → apply → ack.

## Configuration

| Env | Default | Purpose |
|---|---|---|
| `STORE_CODES` | empty | Comma-separated store codes seeded at boot (optional; fleet can be grown via UI/API) |
| `PORT` | `4000` | Dashboard + fleet API port |
| `RABBITMQ_URL` | built from parts | Full AMQP URL (wins over parts) |
| `RABBITMQ_USER/PASSWORD/HOST/PORT` | `admin/admin123/localhost/5674` | Matches fusion-cdh-api local compose |
| `RABBITMQ_SYNC_EXCHANGE` | `cdh.datasync` | Topic exchange |
| `RABBITMQ_SYNC_DLX` | `cdh.datasync.dlx` | Dead-letter exchange |
| `RABBITMQ_SYNC_PREFETCH` | `10` | Per-store channel prefetch |
| `DATA_DIR` | `./data` | Per-store JSON state root |
| `RECONCILE_BASE_URL` | unset | Head-office base URL for snapshot pull |
| `RECONCILE_SNAPSHOT_PATH` | `/api/v1/store-data-sync/snapshot` | Snapshot endpoint path |

## Wire contract

[`src/contracts/data-sync.ts`](src/contracts/data-sync.ts) is **vendored** from
`fusion-cdh-api/libs/contracts/src/data-sync/` (routing-key conventions,
message/ack shapes, content hashing). If the contract changes there, re-sync
this file; `schemaVersion` on the message flags structural drift at runtime.

A real POS integration would replace the two file-backed adapters with its
local database:

- `FileAppliedVersionStore` → applied-version table
- `FileDatasetApplier` → writes into POS tables
