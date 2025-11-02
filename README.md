# torero-mq

Type-safe BullMQ queues you can tame — with Zod‑checked inputs/outputs, idempotent enqueues, repeatable jobs, and an ergonomic instance API.

## What Is This?

`torero-mq` is a thin, type‑safe layer over BullMQ that:

- Validates job inputs (and outputs) using Zod schemas
- Provides a small, instance‑based service API (`QueueService`) with a required `prefix` used to namespace Redis keys
- Supports idempotent publishes via `idempotencyKey`
- Exposes scheduling (`scheduleAt`, `scheduleIn`) and repeatable jobs (`scheduleRepeat` as an async stream)
- Helps avoid duplicate workers in a cluster with a small Redis lock

You bring Redis, BullMQ, and Zod. We make it easy to define queues with types that flow end‑to‑end.

## Requirements

- Node.js: ^18.17, ^20, or >= 22
- Redis: a reachable Redis instance
- Peer dependencies (install in your app): `bullmq`, `ioredis`, `zod`

Install peers and the library:

```
pnpm add bullmq ioredis zod torero-mq
```

## Quick Start

Define a queue, initialize the service, and publish a job:

```
import { z } from 'zod';
import { QueueService } from 'torero-mq';
import IORedis from 'ioredis';

const queues = new QueueService('myapp');

const sumQueue = queues.defineQueue({
  name: 'sum',
  inputSchema: z.object({ a: z.number(), b: z.number() }),
  outputSchema: z.number(),
  async process(input) {
    return input.a + input.b;
  },
});

await queues.initQueues({
  connection: new IORedis(process.env.REDIS_URL!),
  runWorkers: true,
});

const { awaitResult } = await sumQueue.publish({ a: 2, b: 3 });
console.log(await awaitResult()); // 5
```

## Context Propagation (runWithContext)

Wrap job processing in your own context function — useful for AsyncLocalStorage, tracing, etc.

```
import { AsyncLocalStorage } from 'node:async_hooks';

const als = new AsyncLocalStorage<Map<string, string>>();

await queues.initQueues({
  connection: new IORedis(process.env.REDIS_URL!),
  runWorkers: true,
  runWithContext: (fn) => als.run(new Map([['requestId', 'abc-123']]), fn),
});

// Inside your queue `process` function you can read it with als.getStore()
```

## Scheduling and Repeats

Run later at an absolute time:

```
await sumQueue.scheduleAt(new Date(Date.now() + 15_000), { a: 1, b: 4 });
```

Run later after a delay:

```
await sumQueue.scheduleIn(30_000, { a: 3, b: 9 });
```

Repeat with results as a stream:

```
const stream = sumQueue.scheduleRepeat({ everyMs: 5_000 }, { a: 2, b: 2 });

(async () => {
  for await (const { jobId, result } of stream) {
    console.log('repeat result', jobId, result);
    if (/* some condition */ false) {
      await stream.cancel();
      break;
    }
  }
})();
```

## Idempotency and Options

- Use `idempotencyKey` to dedupe enqueue calls:

```
await sumQueue.publish(
  { a: 1, b: 2 },
  { idempotencyKey: ({ a, b }) => `${a}:${b}` }
);
```

- Other useful options (mapped to BullMQ JobsOptions via `toBullOptions`):
    - `retries`, `backoff`, `timeoutMs`, `priority`, `removeOnComplete`, `removeOnFail`

## Worker Management

- `runWorkers: true` makes the service spawn in‑process workers.
- `exclusiveWorkers` (default: true) acquires a lightweight Redis lock so only one worker runs per queue across your cluster.
- If you enable late registration via `allowLateRegistration()`, the service can materialize new queues after init.
- Call `reconcileWorkers()` to attempt to spawn workers for runtimes that are missing one (useful if a lock was released and you want to race for it again).

## API Summary

- `class QueueService(prefix: string, logger?: Logger)`
    - `defineQueue({ name, inputSchema, outputSchema?, process, defaults?, hooks?, init? })`
    - `initQueues({ connection, runWorkers, prefix?, runWithContext?, allowLateRegistration?, exclusiveWorkers? })`
    - `publish(name, input, options?) → AwaitResult`
    - `scheduleAt(name, when, input, options?) → AwaitResult`
    - `scheduleIn(name, delayMs, input, options?) → AwaitResult`
    - `scheduleRepeat(name, spec, input, options?) → RepeatStream`
    - `reconcileWorkers()`
    - `allowLateRegistration()`
    - `materialize(name, { runWorkers? })`
    - `shutdown()`

Key types:

- `PublishOptions<T>` → attempts, backoff, timeoutMs, priority, idempotencyKey, removeOnComplete/Fail
- `QueueDefaults` → per‑queue defaults for the above (plus concurrency and optional limiter)
- `RepeatSpec` → `{ everyMs?: number; cron?: string }`
- `RepeatStream<T>` → async iterable; call `cancel()` to unschedule; `close()` to stop listening
- `AwaitResult<T>` → `{ jobId, awaitResult, cancel }`
- `Logger` → `{ info, warn, error }`

## Development

- Build: `pnpm run build`
- Test: `pnpm test`

## License

MIT — see `LICENSE`.
