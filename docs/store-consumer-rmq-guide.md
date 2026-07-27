# CDH Data-Sync — Store Consumer Integration Guide (NestJS `Transport.RMQ`)

**Audience:** the store API/app team consuming CDH datasets with the official
[NestJS RabbitMQ transport](https://docs.nestjs.com/microservices/rabbitmq)
(`@nestjs/microservices`, `Transport.RMQ`).

**Status:** every code block below was runtime-verified against the CDH broker
with `@nestjs/microservices` **11.1.1**. The `wildcards`, `exchange`, and
`noAssert` options this guide relies on require **NestJS v11+**.

> ⚠️ The default `Transport.RMQ` configuration **will not work** against this
> pipeline — messages will be silently dead-lettered. Two small overrides (a
> deserializer and a serializer) are required. They are explained in
> [The two incompatibilities](#the-two-incompatibilities-you-must-configure-around)
> and included in the reference config.

---

## 1. What you are integrating with

Head office (`fusion-cdh-api`) publishes **versioned dataset messages** to a
durable **topic exchange**. Your store consumes them from **its own durable
queue** and reports back with **acknowledgement messages**. All bodies are
plain JSON — no NestJS message wrapper on the wire.

### Broker topology contract

| Item | Value | Who owns it |
|---|---|---|
| Exchange | `cdh.datasync` — `topic`, durable | Head office (you may assert it identically) |
| Dead-letter exchange | `cdh.datasync.dlx` — `topic`, durable | Head office |
| Your queue | `q.store.<STORE_CODE>` — durable, `arguments: { "x-dead-letter-exchange": "cdh.datasync.dlx" }` | **You** (asserted by your consumer) |
| Your bindings | `dataset.*.global` **and** `dataset.*.store.<STORE_CODE>` | **You** |
| Ack routing key | `dataset.ack.<datasetType>.<STORE_CODE>` → published to `cdh.datasync` | You publish; head office consumes |
| Ack queue | `q.sync.acks` | Head office — **never assert or consume it** |

Rules that keep the fleet healthy:

- **Queue name must be exactly** `q.store.<STORE_CODE>` with the arguments
  above. RabbitMQ rejects re-declaration with different arguments
  (`PRECONDITION_FAILED`), and head office tooling monitors queues by this
  naming convention.
- **Never bind `dataset.#`** — it matches *every* store's STORE-scoped
  datasets, so you would receive (and ack!) other stores' data.
- One publish of a GLOBAL dataset fans out to every bound store queue; a
  STORE dataset reaches only the queue whose binding matches. You do not need
  to know or care which — the two bindings above cover both.

### Message contract — the message you receive

```jsonc
{
  "datasetType": "payment-types",      // see dataset types below
  "scope": "GLOBAL",                   // "GLOBAL" | "STORE"
  "scopeId": null,                     // store code when scope === "STORE", else null
  "version": 4,                        // monotonic per (datasetType, scope, scopeId)
  "previousVersion": 3,                // null for the first version
  "mode": "SNAPSHOT",                  // "SNAPSHOT" | "PARTIAL"
  "contentHash": "<sha256 hex>",       // hash of payload (stable key ordering)
  "schemaVersion": 1,                  // message structure version
  "issuedAt": "2026-07-05T02:00:00.000Z",
  "issuedBy": "system",
  "payload": { /* the dataset */ }     // SNAPSHOT: full dataset; PARTIAL: see below
}
```

Current dataset types: `employees` (STORE), `menu` (STORE), `roles` (GLOBAL),
`payment-types` (GLOBAL), `transaction-types` (GLOBAL),
`cash-denominations` (GLOBAL). New types appear without notice — handle
unknown `datasetType` values gracefully (apply-or-ignore, don't crash).

**You must handle both modes.** Partial-capable datasets (`employees`, `roles`,
`payment-types`) broadcast a PARTIAL whenever the change-set is smaller than the
snapshot; everything else — first versions, rebroadcasts, large change-sets,
`menu` — arrives as a SNAPSHOT. A PARTIAL payload looks like:

```jsonc
{
  "recordsField": "paymentTypes",  // the array inside your stored snapshot to merge into
  "keyField": "code",              // each record's identity field
  "upserts": [ { "code": "GCASH", "name": "GCash", ... } ], // new or changed records
  "deletes": [ "CHEQUE" ]          // keys of removed records
}
```

Wrapper fields outside `recordsField` (e.g. `service`, `createdBy`) are never
changed by a partial. The apply rules for both modes are in
[section 5](#5-applying-rules-your-responsibilities).

### Message contract — the ack you send

After every apply attempt, publish to `cdh.datasync` with routing key
`dataset.ack.<datasetType>.<STORE_CODE>` and a **raw JSON body** (no wrapper):

```jsonc
{
  "storeCode": "KFCCAV03",
  "datasetType": "payment-types",
  "version": 4,
  "status": "APPLIED",                 // "APPLIED" | "FAILED"
  "contentHash": "<from the message>",// optional
  "error": "why it failed"             // only when FAILED
}
```

Head office records this in its `store_sync_state` table — it is how the CDH
team sees your store's sync health. Missing acks look like a broken store.

---

## 2. Installation

```bash
npm i @nestjs/microservices amqplib amqp-connection-manager
```

Connection URL (per environment, from the CDH team):

```
amqp://<user>:<password>@<host>:<port>
```

---

## 3. The two incompatibilities you must configure around

The transport works Nest-to-Nest by wrapping every message in
`{ "pattern": ..., "data": ... }`. This pipeline uses **raw JSON bodies**, so:

1. **Inbound:** Nest's default deserializer finds no `pattern` field in our
   messages, fails to match any handler, and **nacks the message to the
   dead-letter exchange**. You will see
   `There is no matching event handler defined in the remote service`
   warnings while your data silently drains into the DLX.
   → Fix: a ~10-line custom `deserializer` that derives the pattern from the
   message itself (below).

2. **Outbound:** `ClientProxy.emit(pattern, data)` would publish
   `{"pattern":"...","data":{...}}` — head office's ack consumer parses the
   body as a raw ack and would reject it.
   → Fix: a one-line pass-through `serializer` so only `data` is sent. With
   `wildcards: true`, the emit *pattern* becomes the AMQP *routing key*, which
   is exactly what the ack contract needs.

---

## 4. Reference configuration (runtime-verified)

### 4.1 The deserializer

```ts
// sync-message.deserializer.ts
import { Deserializer } from '@nestjs/microservices';

/**
 * Maps raw CDH messages to Nest's { pattern, data } packet. The pattern is
 * reconstructed from the message so Nest's wildcard matching can dispatch it
 * to the right @EventPattern handler. Anything that isn't a message gets
 * pattern: undefined → Nest nacks it to the dead-letter exchange.
 */
export class SyncMessageDeserializer implements Deserializer {
  deserialize(value: any) {
    if (value && typeof value === 'object' && value.datasetType) {
      const pattern =
        value.scope === 'GLOBAL'
          ? `dataset.${value.datasetType}.global`
          : `dataset.${value.datasetType}.store.${value.scopeId}`;
      return { pattern, data: value };
    }
    return { pattern: undefined, data: value };
  }
}
```

### 4.2 The microservice (consumer)

```ts
// main.ts
import 'dotenv/config'; // MUST run before controllers are imported — see note below
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { SyncMessageDeserializer } from './sync-message.deserializer';

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.RMQ,
    options: {
      urls: [process.env.CDH_RABBITMQ_URL!],
      queue: `q.store.${process.env.STORE_CODE}`,
      queueOptions: {
        durable: true,
        arguments: { 'x-dead-letter-exchange': 'cdh.datasync.dlx' },
      },
      exchange: 'cdh.datasync',
      exchangeType: 'topic',
      wildcards: true,   // handler patterns become queue bindings + regex dispatch
      noAck: false,      // manual ack — you MUST ack/nack in every handler
      prefetchCount: 10,
      deserializer: new SyncMessageDeserializer(),
    },
  });
  await app.listen();
}
void bootstrap();
```

(If your app is also an HTTP server, use `app.connectMicroservice(...)` +
`app.startAllMicroservices()` — the options are identical.)

### 4.3 The handlers

```ts
// data-sync.controller.ts
import { Controller, Inject } from '@nestjs/common';
import { ClientProxy, Ctx, EventPattern, Payload, RmqContext } from '@nestjs/microservices';
import { lastValueFrom } from 'rxjs';

const STORE_CODE = process.env.STORE_CODE!; // decorator args resolve at import time

@Controller()
export class DataSyncController {
  constructor(
    @Inject('DATASYNC_ACK_CLIENT') private readonly ackClient: ClientProxy,
    private readonly applier: DatasetApplierService, // your local-DB writer
  ) {}

  @EventPattern('dataset.*.global')
  async onGlobalDataset(@Payload() message: any, @Ctx() context: RmqContext) {
    return this.handle(message, context);
  }

  @EventPattern(`dataset.*.store.${STORE_CODE}`)
  async onStoreDataset(@Payload() message: any, @Ctx() context: RmqContext) {
    return this.handle(message, context);
  }

  private async handle(message: any, context: RmqContext) {
    const channel = context.getChannelRef();
    const message = context.getMessage();
    try {
      const outcome = await this.applier.apply(message); // 'applied' | 'skipped'
      channel.ack(message);
      if (outcome === 'applied') {
        await this.sendAck(message, 'APPLIED');
      }
    } catch (error) {
      channel.nack(message, false, false); // requeue=false → dead-letter exchange
      await this.sendAck(message, 'FAILED', (error as Error).message);
    }
  }

  private async sendAck(message: any, status: 'APPLIED' | 'FAILED', error?: string) {
    await lastValueFrom(
      this.ackClient.emit(`dataset.ack.${message.datasetType}.${STORE_CODE}`, {
        storeCode: STORE_CODE,
        datasetType: message.datasetType,
        version: message.version,
        status,
        contentHash: message.contentHash,
        ...(error ? { error } : {}),
      }),
    );
  }
}
```

Two footguns here:

- **`@EventPattern` arguments are evaluated when the file is imported.** If
  `STORE_CODE` comes from a `.env` file, load it *before* the controller is
  imported (`import 'dotenv/config'` first in `main.ts`). `ConfigModule` alone
  is too late — it loads during bootstrap, after decorators have run.
- **`noAck: false` means Nest never acks for you.** Every code path through a
  handler must end in exactly one `channel.ack(...)` or
  `channel.nack(..., false, false)`, or messages sit unacked until restart and
  then redeliver.

### 4.4 The ack client

```ts
// app.module.ts (imports array)
ClientsModule.register([
  {
    name: 'DATASYNC_ACK_CLIENT',
    transport: Transport.RMQ,
    options: {
      urls: [process.env.CDH_RABBITMQ_URL!],
      exchange: 'cdh.datasync',
      wildcards: true,  // emit(pattern, ...) publishes to the exchange with pattern as routing key
      noAssert: true,   // client must not assert a queue of its own
      persistent: true,
      serializer: { serialize: (packet: any) => packet.data }, // raw body — no {pattern,data} wrapper
    },
  },
]),
```

Notes:

- `wildcards: true` on a client changes `emit()` from "send to queue" to
  "publish to `exchange` with the emit pattern as routing key". Without it the
  ack would be pushed into a queue named after `options.queue` and never reach
  head office.
- `noAssert: true` stops the client from asserting a default queue you don't
  need.
- `emit()` returns a cold Observable — nothing is published until it is
  subscribed. `await lastValueFrom(...)` (as above) or `.subscribe()`.

---

## 5. Applying rules (your responsibilities)

The pipeline is **at-least-once**: redeliveries, replays, and re-publishes of
the same version are normal. Your applier must be idempotent. Persist, per
`datasetType`, the last applied `version` (and ideally `contentHash`) in your
local database, then:

1. **`message.version <= appliedVersion`** → do nothing, `ack`, no ack
   message needed (it's a replay).
2. **`mode === 'SNAPSHOT'`** → replace your local copy of the dataset
   wholesale with `payload`, record the new version, `ack`, send `APPLIED`.
3. **`mode === 'PARTIAL'` and `message.previousVersion === appliedVersion`** →
   merge into your stored snapshot's `payload.recordsField` array, matching
   records by `payload.keyField`: replace-or-insert every record in `upserts`,
   remove every key in `deletes`, keep all other wrapper fields untouched.
   Record the new version, `ack`, send `APPLIED`.
4. **`mode === 'PARTIAL'` on any other base version (a gap)** → do **not**
   apply. `nack(msg, false, false)` and recover via a fresh snapshot (see
   catch-up below). Applying a partial onto the wrong base silently corrupts
   your data.
5. **Apply throws** → `nack(msg, false, false)` + send `FAILED` with the error
   message. The message dead-letters to `cdh.datasync.dlx`; head office sees
   the failure in `store_sync_state`.

Update the stored version and the dataset **in the same local transaction**,
so a crash between the two can't desynchronize them.

Optional but recommended integrity check: recompute the payload hash and
compare with `message.contentHash` before applying —
`sha256(stableStringify(payload))` where `stableStringify` serializes objects
with keys sorted recursively (arrays in order). Reject mismatches as failures.

## 6. First run and catch-up

Your queue starts existing when your consumer first connects — everything
published *before* that moment never reaches it. Likewise, if your store is
offline for a long period the queue keeps accumulating (that's by design;
you'll catch up on reconnect), but a *brand-new* store needs initial state.

Until the head-office snapshot pull endpoint ships (planned:
`GET /store-data-sync/snapshot` on the store gateway), ask the CDH team to
trigger a sync for your store after your consumer is connected — every dataset
arrives as a fresh SNAPSHOT. The version guard makes this safe to repeat.

## 7. Quick checklist

- [ ] `@nestjs/microservices` v11+, `amqplib`, `amqp-connection-manager` installed
- [ ] Queue `q.store.<STORE_CODE>`, durable, with the `x-dead-letter-exchange` argument — never other args
- [ ] `wildcards: true`, `exchange: 'cdh.datasync'`, `exchangeType: 'topic'`, `noAck: false`
- [ ] Custom deserializer registered (inbound) — without it everything dead-letters
- [ ] Exactly two handler patterns: `dataset.*.global` and `dataset.*.store.<STORE_CODE>` — never `dataset.#`
- [ ] Every handler path acks or nacks exactly once
- [ ] Ack client: `wildcards: true` + `noAssert: true` + pass-through serializer; `emit` awaited
- [ ] Applied version persisted per dataset type, same transaction as the data
- [ ] `STORE_CODE` available at import time (dotenv loaded first)
- [ ] Never touch `q.sync.acks`

## 8. Verifying your integration

1. Start your consumer; confirm in the RabbitMQ management UI that
   `q.store.<STORE_CODE>` exists with both bindings and one consumer.
2. Ask the CDH team to trigger a sync for a GLOBAL dataset and one
   STORE-scoped dataset for your store code.
3. Confirm your handlers fire and your local data updates.
4. Ask the CDH team to check `store_sync_state` — your store code should show
   `APPLIED` with the right versions. That row is the definition of "done".

For reference, a working consumer implementation of this exact contract (raw
`amqp-connection-manager`, not the Nest transport) lives in the
`fusion-cdh-store-consumer` repo, including a fleet simulator you can run
against the same broker to compare behavior.
