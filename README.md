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
from head office — either the **Trigger a sync** panel at the top of the
dashboard (see "Self-serve trigger" below; needs `GATEWAY_ADMIN_*` configured),
or `POST /api/v1/store-data-sync/payment-types` on app-gateway's Swagger — and
watch each store apply it and ack.

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
| `POST /api/fleet/stores/:code/stop` | Pause consuming — stays listed as "stopped", queue and registry entry kept |
| `POST /api/fleet/stores/:code/start` | Resume consuming for a stopped store |
| `DELETE /api/fleet/stores/:code` | Remove a store from the fleet entirely (untracked, won't rejoin on restart); **keeps** its queue |
| `DELETE /api/fleet/stores/:code?delete=true` | Same, and also deletes the broker queue |
| `POST /api/fleet/stores/:code/purge` | Empty the store's pending messages; queue, bindings, and consumer are kept |
| `GET /api/fleet/stores/:code/datasets/:type` | Inspect a store's applied dataset |
| `DELETE /api/fleet/stores/:code/datasets/:type` | Wipe one locally applied dataset (state + file only) — re-applies from the next matching message |
| `DELETE /api/fleet/stores/:code/datasets` | Wipe every locally applied dataset for the store |
| `POST /api/fleet/stores/:code/inject-failure` `{"datasetType":"employees","times":1}` | Force the next N apply(s) of a dataset type to fail — produces a real `FAILED` ack |
| `DELETE /api/fleet/stores/:code/inject-failure/:type` | Clear a pending failure injection before it fires |
| `GET /api/trigger/status` | Whether the self-serve trigger is configured (see below) |
| `POST /api/trigger/:dataset` `{"storeCode":"S001","isTest":false}` | Proxy to app-gateway's real `POST /store-data-sync/:dataset` |

Adding a store declares its durable queue immediately, so it receives
everything published from that moment; history before that needs a fresh
publish (there's no history backfill — see "Gaps" below). Local `data/<code>/`
files are kept on remove for post-mortem — delete manually if unwanted.

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
connect; anything published **before** that isn't recovered automatically —
trigger a fresh publish for it (see "Gaps" below).

## Deploying with PM2 (shared staging box)

For a persistent shared tester running alongside a PM2-managed `fusion-cdh-api`
on the same host, `pm2.config.js` mirrors that repo's own ecosystem file
convention (`dist/main.js`, `./logs/*.log`, `autorestart`, `max_memory_restart`):

```bash
npm install
npm run build
pm2 start pm2.config.js   # run from this repo's root, same as fusion-cdh-api
```

Notes:

- `.env` on the box supplies config the same way it does for `fusion-cdh-api`'s
  own PM2 apps — `ConfigModule.forRoot()` loads it from the process's cwd, so
  no `env:` block is needed in `pm2.config.js`. **Do not commit it** (already
  gitignored); if RabbitMQ/app-gateway run on that same host, the
  `.env.example` defaults (`127.0.0.1`/`localhost`) need no changes.
- `DATA_DIR` (default `./data`) sits outside `dist/`, so `nest build`'s
  `deleteOutDir` never touches it — fleet state survives rebuilds/restarts.
  Redeploys are `git pull && npm run build && pm2 restart fusion-cdh-store-consumer`.
- **This dashboard has no login.** Nothing in front of `PORT` (default `4000`)
  stops anyone who can reach it from triggering real syncs, injecting
  failures, or deleting broker queues. Firewall the port to your internal
  network/VPN, or put a reverse proxy with basic auth in front — don't expose
  it publicly as-is.
- `GATEWAY_ADMIN_PASSWORD` is a real account's plaintext password sitting in
  that server's `.env`. Treat the box accordingly (limit who has shell/SSH
  access to it) — there's no secrets manager integration here, just a `.env`
  file, matching how `fusion-cdh-api` itself handles secrets on that host.

## How applying works

- **Version guard** — `message.version <= applied.version` is acked and
  skipped (idempotent replays).
- **SNAPSHOT** — payload verified against `contentHash` (same stable-stringify
  SHA-256 as head office) and written wholesale.
- **PARTIAL** — applied only when `previousVersion` matches the applied version;
  `{ upserts, deletes }` merged into the stored snapshot's `recordsField` array,
  matching records by `keyField`.
- **Gap** (partial on a stale base) — never applied: the message is dead-lettered
  and the gap is only ever surfaced, never auto-resolved (see "Gaps" below).
- **Failure** — message dead-lettered to `cdh.datasync.dlx`, `FAILED` ack sent
  with the error, visible in head office's `store_sync_state`.

## Gaps (a PARTIAL landing on a stale base)

There is deliberately **no automatic recovery** for a gap. When a store is
behind and receives a PARTIAL it can't apply (`previousVersion` doesn't match
what it has), the consumer:

- dead-letters the message (nack, no requeue — it goes to `cdh.datasync.dlx`),
- logs a `WARN ... Gap on <dataset>: partial expects vN but at vM`,
- and records a `gap` event visible on the dashboard (per-store event feed /
  "Last activity" column).

That's the whole signal — it's the tester's cue to fix it themselves: trigger
a real (non-preview) sync for that dataset via app-gateway's Scalar/Swagger
API (or this dashboard's Trigger panel, if configured — see below, it calls
the same endpoint). A triggered sync always rebroadcasts a full **SNAPSHOT**,
never a PARTIAL, so it's gap-safe regardless of how far behind the store is.

## Fault injection (testing the FAILED-ack path)

A `FAILED` ack normally only happens on a genuine bug (payload/hash mismatch,
a merge error). To make that path testable on demand — e.g. to confirm head
office's `store_sync_state` correctly flips a store to `FAILED` and that any
alerting on top of it fires — arm a store to force its next apply(s) of a
dataset type to fail:

- **Dashboard:** row action **Fail…** → enter a dataset type (`employees`,
  `menu`, `roles`, `payment-types`, `store`) and how many applies should fail.
  An armed dataset shows as a `⚠ type ×N` chip next to that store's applied
  datasets; click the chip to clear it before it fires.
- **API:** `POST /api/fleet/stores/:code/inject-failure {"datasetType":"...","times":1}`,
  cleared with `DELETE /api/fleet/stores/:code/inject-failure/:type`.

Arming works even for a dataset the store has never applied yet, so you can
also force a store's *very first* apply of something to fail. The injected
failure consumes the real apply path (nack to the DLX, `FAILED` ack sent,
event logged) — it's not a mock, it's the same failure handling a real
corrupt payload would hit. Injections are in-memory and per-process; they
don't survive a restart and aren't persisted to `data/`.

## Self-serve trigger (drive scenarios without Swagger)

By default this tool only *observes* the store side — every scenario still
needed a separate app-gateway Swagger tab to actually kick a sync off. Setting
three env vars turns on a **Trigger a sync** panel at the top of the dashboard
that calls app-gateway's real, guarded `POST /store-data-sync/<dataset>`
endpoints directly (including `isTest` preview mode), so a full scenario pass
can run from one tool:

```bash
GATEWAY_ADMIN_BASE_URL=http://localhost:1000
GATEWAY_ADMIN_USERNAME=<a real, verified CDH username>
GATEWAY_ADMIN_PASSWORD=<their password>
GATEWAY_ADMIN_DEVICE_NAME=fusion-cdh-store-consumer   # optional, this is the default
```

Leave `GATEWAY_ADMIN_BASE_URL` unset (the default) and the panel simply stays
hidden — nothing else about the tool changes. Notes:

- This is a **real sign-in** through app-gateway's `/auth/sign-in`, using an
  actual account — there's no service-account/API-key path in the pipeline
  today, so point it at a dedicated test user, not someone's personal login.
  The account must be verified (`isVerified`); the sync endpoints reject
  unverified users.
- The session (access + refresh token) is cached in memory and reused/refreshed
  across triggers — it does **not** sign in on every click. This matters
  because head office locks an account out after repeated failed sign-ins, and
  every sign-in adds a session row; both are cheap to avoid by reusing one
  session per process lifetime. A 401 from a stale/revoked session triggers
  exactly one fresh sign-in + retry.
- Credentials live only in `.env` (gitignored) and are never sent to the
  browser — the dashboard talks to this app's own `/api/trigger/*`, which
  holds the token server-side and proxies the call.
- GLOBAL datasets (`roles`, `payment-types`) don't take a store code; the
  panel disables that field automatically when you pick one.

## Simulating a version gap

Forcing a gap on demand (rather than waiting to time a real head-office change
against a store that happens to be behind) is deterministic:

1. Pick a dataset that supports PARTIAL diffs on head office — `employees`,
   `roles`, or `payment-types` (`menu` and `store` are snapshot-only and can
   never gap).
2. Wipe that dataset's local state for one store: dashboard chip → **Wipe**,
   or `DELETE /api/fleet/stores/:code/datasets/:type`. This resets the
   store's applied version to 0.
3. Make any real content change to that dataset at head office (e.g. edit a
   payment type) and let it publish normally. Because the store is wiped
   (applied version 0) but head office's next publish is a PARTIAL with a
   `previousVersion` ≥ 1, the versions can never line up — the store detects
   the gap: nacks the message and logs/records the `gap` event (see "Gaps"
   above).
4. Resolve it the same way any gap gets resolved: trigger a real sync for
   that dataset via Swagger or the Trigger panel.

## Known gaps

- **STORE-scoped datasets need real store records.** `employees`/`menu`/`store`
  sync 404s at head office before anything reaches RabbitMQ unless the store
  code you're testing with (e.g. the `STORE_CODES` in `.env`) resolves to an
  actual, non-draft store in the CDH `store-service` database.

## Scenario playbook

Maps directly to the six test scenarios in `fusion-cdh-api`'s
`docs/DATA_SYNC_BUSINESS_DOCUMENTATION.md`:

| # | Scenario | How to run it here |
|---|---|---|
| 1 | Preview only | Check **Preview only (isTest)** in the Trigger panel and submit (or `isTest: true` on Swagger, if the trigger panel isn't configured). Nothing to check in this tool — success means **no** change appears on the dashboard; the response dialog shows the would-be payload only. |
| 2 | Real update, one restaurant | Trigger panel: pick a STORE-scoped dataset, enter one store code (make sure it's a real, seeded store — see Known gaps), leave preview unchecked. Watch that store's chip version bump and its event feed show `applied`. |
| 3 | Repeat, no change | Trigger the same dataset again unchanged. Dashboard event feed shows `skipped ... (already at vN)`; version doesn't move. |
| 4 | Company-wide broadcast | Trigger panel: pick `roles` or `payment-types` (store code field auto-disables). Every store in the fleet updates within the 2s dashboard refresh. |
| 5 | Offline catch-up | **Stop** a store (row action), trigger an update, then **Start** it again — the queue held the message; it applies on resume. For a true version-gap variant (not just a paused consumer), see "Simulating a version gap" above — recovery there is a manual re-trigger, not automatic. |
| 6 | Status check | `GET /api/fleet` (or the dashboard) shows each store's applied version/hash/timestamp per dataset directly — no need to ask the restaurant. |

All six now run entirely from this dashboard once `GATEWAY_ADMIN_*` is set (see
"Self-serve trigger" above); without it, steps that say "Trigger panel" fall
back to app-gateway's Swagger UI instead.

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
| `GATEWAY_ADMIN_BASE_URL` | unset | app-gateway base URL; unset disables the self-serve trigger panel entirely |
| `GATEWAY_ADMIN_USERNAME` / `GATEWAY_ADMIN_PASSWORD` | unset | Credentials for a real, verified CDH user used to call the trigger endpoints |
| `GATEWAY_ADMIN_DEVICE_NAME` | `fusion-cdh-store-consumer` | `deviceName` sent on sign-in (shows up in that user's active sessions) |

## Wire contract

[`src/contracts/data-sync.ts`](src/contracts/data-sync.ts) is **vendored** from
`fusion-cdh-api/libs/contracts/src/data-sync/` (routing-key conventions,
message/ack shapes, content hashing). If the contract changes there, re-sync
this file; `schemaVersion` on the message flags structural drift at runtime.

A real POS integration would replace the two file-backed adapters with its
local database:

- `FileAppliedVersionStore` → applied-version table
- `FileDatasetApplier` → writes into POS tables
