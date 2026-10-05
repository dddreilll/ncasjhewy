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
  "schemaVersion": 2,                  // message structure version
  "issuedAt": "2026-07-05T02:00:00.000Z",
  "issuedBy": "system",
  "payload": { /* the dataset */ }     // SNAPSHOT: full dataset; PARTIAL: see below
}
```

Current dataset types: `employees` (STORE), `menu` (STORE), `store-profile` (STORE),
`store-configurations` (STORE), `roles` (GLOBAL), `payment-types` (GLOBAL),
`transaction-types` (GLOBAL), `cash-denominations` (GLOBAL), `assets` (GLOBAL),
`events` (GLOBAL), `event-groups` (GLOBAL).

> **Renamed:** the store profile dataset was `store` and is now `store-profile`.
> If your app reads the store profile by dataset type, switch to
> `store-profile`; data you hold under `store` is no longer updated. (A type
> named `store` made your own acks match your queue's `dataset.*.store.<code>`
> binding, so they looped back to you.)

New types appear without notice — handle unknown `datasetType` values
gracefully (apply-or-ignore, don't crash).

`event-groups` carries every non-deleted group, including hidden ones — filter on
`isStoreVisible` and `effectiveFrom`/`effectiveTo` locally. Each event's
`eventGroupCode` in `events` resolves against `event-groups[].code`.

**You must handle both modes.** Partial-capable datasets (`employees`, `roles`,
`payment-types`, `transaction-types`, `cash-denominations`, `assets`,
`store-configurations`, `events`, `event-groups`) broadcast a PARTIAL whenever the change-set is smaller
than the snapshot; everything else — first versions, rebroadcasts, large
change-sets, `menu`, `store-profile` — arrives as a SNAPSHOT. A PARTIAL payload looks
like:

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

For **every message you could read** — applied, skipped or failed — publish
to `cdh.datasync` with routing key `dataset.ack.<datasetType>.<STORE_CODE>`
and a **raw JSON body** (no wrapper):

```jsonc
{
  "storeCode": "KFCCAV03",
  "datasetType": "payment-types",
  "version": 4,                        // APPLIED/SKIPPED: the version you now hold
  "status": "APPLIED",                 // "APPLIED" | "SKIPPED" | "FAILED"
  "contentHash": "<of that version>",  // optional
  "error": "why it failed"             // FAILED only: the reason head office shows
}
```

| Status    | When                                                        |
| --------- | ----------------------------------------------------------- |
| `APPLIED` | You applied this version                                    |
| `SKIPPED` | You already hold this version or a newer one (a replay)     |
| `FAILED`  | You read the message but couldn't apply it — say why in `error` |

Head office records this in its `store_sync_state` table and its sync event
log — it is how the CDH team sees your store's sync health, and a `FAILED`
ack's `error` is the reason they see. Missing acks look like a broken store.

**Never nack a message you could read.** Report a failure with a `FAILED`
ack, then ack the message. Nack (without requeue) only a message you can't
read at all; RabbitMQ then dead-letters it, and head office sees it as failed
with no reason.

#### How head office checks your ack

Head office validates every ack before it records anything. It **rejects**
an ack when:

- the body isn't a JSON object;
- `storeCode`, `datasetType`, `version` or `status` is missing or empty;
- `version` isn't an integer of at least 1 (`"4"` as a string is rejected);
- `status` isn't one it knows: `APPLIED`, `SKIPPED`, `FAILED` (or `PENDING`);
- `contentHash` or `error` is present but isn't a string.

A rejected ack changes nothing for your store. Head office dead-letters it,
its Sync Event Logs show it as a failed event from RabbitMQ (reason
`rejected`), and only head office's service log says what was wrong. Until a
valid ack arrives, your store keeps its last valid status, or stays "Not
confirmed".

Head office also ignores an ack for a version **older** than the one it
already recorded as applied for your store (a late or out-of-order ack). It
logs the ack as an event, but the ack doesn't move your store's status
back.

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
    const rawMessage = context.getMessage();
    let held: { version: number; contentHash?: string; skipped: boolean };
    try {
      // Returns the version + hash you now hold, and whether this was a skip
      // (rule 1 in section 5) — then it is what you already had, not
      // message.version. Throws on a gap (rule 4) or any apply error.
      held = await this.applier.apply(message);
    } catch (error) {
      // Read but not applied: report FAILED with the reason FIRST, then ack.
      // A crash in between redelivers the message instead of losing the failure.
      try {
        await this.sendAck(message.datasetType, message.version, 'FAILED',
          message.contentHash, (error as Error).message);
        channel.ack(rawMessage);
      } catch {
        // The FAILED ack couldn't be sent: dead-letter the message instead,
        // so head office still sees a failure (without the reason).
        channel.nack(rawMessage, false, false);
      }
      return;
    }
    channel.ack(rawMessage);
    // Sent on apply AND on skip. Keep it out of the try: the message is
    // already acked, so a failed publish must not turn into a FAILED ack.
    // A lost APPLIED/SKIPPED is re-sent on the next redelivery.
    const status = held.skipped ? 'SKIPPED' : 'APPLIED';
    await this.sendAck(message.datasetType, held.version, status, held.contentHash)
      .catch((error) => console.warn(`${status} ack not sent: ${(error as Error).message}`));
  }

  private async sendAck(
    datasetType: string,
    version: number,
    status: 'APPLIED' | 'SKIPPED' | 'FAILED',
    contentHash?: string,
    error?: string,
  ) {
    await lastValueFrom(
      this.ackClient.emit(`dataset.ack.${datasetType}.${STORE_CODE}`, {
        storeCode: STORE_CODE,
        datasetType,
        version,
        status,
        ...(contentHash ? { contentHash } : {}),
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
  then redeliver. A message you could read always ends in `ack` — failures
  included; `nack` is only for one you can't read, or a `FAILED` ack you
  couldn't send.

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

1. **`message.version <= appliedVersion`** → it's a replay: don't apply,
   `ack`, then send `SKIPPED` with **your stored** `appliedVersion` and its
   `contentHash`, not the message's. Head office may have missed your earlier
   ack, so this re-confirms where you are. Never echo `message.version` here:
   on an older replay it reports a version you don't hold. Head office ignores
   an ack older than what it recorded as applied, so the echo only adds a
   misleading event, and it can't confirm a newer version you hold.
2. **`mode === 'SNAPSHOT'`** → replace your local copy of the dataset
   wholesale with `payload`, record the new version, `ack`, send `APPLIED`.
3. **`mode === 'PARTIAL'` and `message.previousVersion === appliedVersion`** →
   merge into your stored snapshot's `payload.recordsField` array, matching
   records by `payload.keyField`: replace-or-insert every record in `upserts`,
   remove every key in `deletes`, keep all other wrapper fields untouched.
   Record the new version, `ack`, send `APPLIED`.
4. **`mode === 'PARTIAL'` on any other base version (a gap)** → do **not**
   apply — applying a partial onto the wrong base silently corrupts your data.
   Send `FAILED` with an `error` naming the gap (e.g. `"Gap on menu: partial
   expects base v6 but store is at v4"`), then `ack`. Head office shows your
   store as failed, and its Fix issues sends you a full SNAPSHOT, which is
   always safe to apply.
5. **Apply throws** → send `FAILED` with the error message, then `ack`. Head
   office shows the failure, with your message as the reason, in
   `store_sync_state` and its sync event log.
6. **The message can't be read at all** (not JSON, no `datasetType`) → there
   is nothing to ack about: `nack(msg, false, false)`. It dead-letters to
   `cdh.datasync.dlx`, and head office logs it as failed with no reason.

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
- [ ] Every handler path acks or nacks exactly once; a readable message is never nacked (failures send `FAILED`, then ack)
- [ ] Ack client: `wildcards: true` + `noAssert: true` + pass-through serializer; `emit` awaited
- [ ] Applied version persisted per dataset type, same transaction as the data
- [ ] Skipped replays send `SKIPPED` with your stored version + hash
- [ ] Gaps and apply errors send `FAILED` with the reason in `error`, then ack
- [ ] Every ack is a JSON object with `storeCode`, `datasetType`, an integer `version` of at least 1 and a known `status`; head office rejects anything else
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
5. Trigger the same sync again unchanged: your handlers should skip it and
   head office should log a `SKIPPED` confirmation.

For reference, a working consumer implementation of this exact contract (raw
`amqp-connection-manager`, not the Nest transport) lives in the
`fusion-cdh-store-consumer` repo, including a fleet simulator you can run
against the same broker to compare behavior.
