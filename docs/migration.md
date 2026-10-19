# Migrating from 0.1

Version 0.2 requires Node.js 26+, BullMQ 6 and, for ordinary connection options, ioredis 6. The old `QueueService` interface and serialized jobs are incompatible with this release.

## Plan the deployment boundary

Do not point the new workers at queues containing 0.1 payloads. Torero now writes versioned input/output envelopes with a rich-value codec. There is no automatic data migration.

Choose either a new Redis prefix for the rewritten application, or stop producers and drain the old workers/queues before switching. Account for delayed jobs, retrying jobs and recurring schedules as well as waiting jobs. Retained old results cannot be read by the new client. Recreate future schedules explicitly and prevent the old and new schedulers from both delivering them. Keep the old deployment available long enough to finish or deliberately retire its remaining work.

Within the new API, an explicit job name and version identify its protocol. Incrementing a version is not an automatic migration or a separate queue; incompatible producers/workers sharing a queue will reject each other's envelopes. Use a new name or prefix when incompatible versions must coexist.

## Replace registration with shared contracts

| 0.1                                                         | Rewrite                                                                              |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `new QueueService(...)`                                     | `createQueueClient(router, settings)` and `createWorkers(implementations, settings)` |
| `defineQueue({ name, inputSchema, outputSchema, process })` | `job(name).input(schema).output(schema)` plus `.handler(...)`                        |
| module augmentation / `getQueue()`                          | inferred nested `client.jobs` router                                                 |
| `initQueues()`                                              | explicit worker `start()`; client lazy start or `start()`                            |
| `publish()` result with `jobId` / `awaitResult()`           | handle with `id` / `wait()`                                                          |
| `scheduleAt` / `scheduleIn`                                 | `schedule(input, { at })` / `schedule(input, { delayMs })`                           |
| `scheduleRepeat`                                            | `schedules.upsert(id, spec, input)` and a separate subscription                      |
| input-derived `idempotencyKey`                              | explicit native BullMQ `jobId` or `deduplication`                                    |

Contracts can live in a shared package using `torero-mq/contract`, which has no BullMQ/Redis runtime imports. A schema package is your application's dependency; Torero uses Standard Schema and does not require Zod. Schemas may have distinct raw input, parsed input, handler output and transformed output types.

Nested paths are only an API organization tool. The explicit `job('stable.name')` supplies the Redis queue name. Avoid duplicate names within a router. Definitions and fluent builders are immutable and do not start connections or processing.

## Separate policy from implementation

Put native retry/retention defaults in `.options({ attempts, backoff, removeOnComplete, removeOnFail })`. Set processing policy with `implement(contract).worker({ concurrency, limiter, timeoutMs }).handler(...)`. Supply root context with `createWorkers(..., { context: async ({ job, signal }) => ... })`; declare its type using `implement(contract).$context<Context>()`. Middleware can enrich it with `.use(async ({ context, next }) => next({ context: { ... } }))`.

Handlers receive one object: `{ input, context, job, signal }`. Use `job.id`, `job.name`, `job.attemptsMade`, `job.updateProgress()` and `job.log()` instead of relying on a raw BullMQ processor argument. Use `await queue.raw()` for administration, with awareness that its jobs contain Torero wire envelopes.

## Validation and serialization changed

The producer snapshots and encodes raw input before async validation, so subsequent caller mutation cannot change the enqueued payload. Producer validation does not replace the stored raw input with its transformed result. Each attempt parses the original input once; transforms therefore do not compound across retries. Validation can run at publication and each attempt, so validators should be pure. Both input and output schemas support async validation/transformation.

Wire values preserve null, undefined, dates, bigints, maps, sets, regular expressions and special numbers. Functions, symbols and custom class instances are rejected. Invalid wire envelopes, invalid schema results and codec errors are permanent failures rather than repeatedly retrying poison jobs.

`new JobError('DOMAIN_CODE', data)` also fails permanently by default. Set `{ retryable: true }` for a transient domain failure. Consumers receive a `JobFailedError`; inspect `details.code` and `details.data` after narrowing unknown data. Contracts infer successful results, not Promise rejection types.

## Waiting, retention and cancellation

`publish()` returns a typed handle. `get(id)` creates that handle synchronously without fetching Redis. `wait({ timeoutMs, signal })` observes completion and checks persisted state, so it also works for fast jobs, late listeners and a new client process while the job remains retained. `result()` reads only an already completed result and throws for a pending, failed or missing job. `status()` returns BullMQ state or `'unknown'`; `retry()` retries a failed job.

Defaults retain completed jobs for 24 hours / 1,000 jobs and failed jobs for 7 days / 5,000 jobs. BullMQ eviction is lazy and best-effort; those limits do not promise a minimum availability window. Override retention deliberately. Immediate removal on completion makes late result reads unavailable.

A wait timeout or abort does not cancel the job. `remove()` returns `'removed'`, `'missing'` or `'active'`; active work continues. Worker deadlines abort `signal` but wait for the handler to settle before releasing its execution slot/lock or starting a retry. Pass the signal into downstream I/O and check it in loops. Ignoring the signal can hold graceful shutdown indefinitely. Do not assume a deadline prevents a late external side effect.

## Schedules and streams

Recurring schedules have explicit public IDs. Use `{ everyMs }` or `{ cron, timezone? }` with optional `startDate`, `endDate` and `limit`. `schedules.upsert()` replaces/configures a scheduler; `schedules.get(id)` returns a handle, and `schedules.list()` exposes schedule metadata.

Subscribe before upsert if the immediate first run matters. Iterators deliver future completed and failed events only, with no replay guarantee. Their capacity is bounded; overflow throws `StreamOverflowError` instead of silently dropping entries. Closing the iterator stops observation. Removing the scheduler stops future scheduled work and closes its associated observers; it does not undo completed side effects.

## Connections, delivery and shutdown

A producer starts lazily, while workers require `start()`. Use a common explicit prefix and connection. Plain options create connections that Torero owns. If you supply an existing Redis client, you retain ownership and must close it yourself; Torero never quits it. A borrowed worker connection must have `maxRetriesPerRequest: null`.

Close worker groups and clients in `finally` or during process shutdown. Closed instances cannot be restarted; create new ones. Close subscriptions when consumers stop reading.

BullMQ remains at-least-once. Native deduplication and bulk enqueueing do not make provider calls exactly-once or create a transaction with your business database. Keep transactional outboxes, unique business constraints and provider idempotency keys where your application needs them.
