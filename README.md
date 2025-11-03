# torero-mq

Type-safe BullMQ queues you can tame — with Zod‑checked inputs/outputs, idempotent enqueues, repeatable jobs, and an ergonomic instance API.

## What Is This?

`torero-mq` is a thin, type‑safe layer over BullMQ that:

- Validates job inputs (and outputs) using Zod schemas
- Provides a small, instance‑based service API (`QueueService`) with a required `prefix` used to namespace Redis keys
- Returns a friendly, fully‑typed `ConcreteQueue` from `defineQueue` that you can import and use anywhere in your app
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

The heart of this library is the `ConcreteQueue` you get back from `defineQueue`. Define once, import anywhere.

1. Create a single service and an init helper:

```
// queues/index.ts
import IORedis from 'ioredis';
import { QueueService } from 'torero-mq';

export const queues = new QueueService({
  prefix: 'myapp',
  runWorkers: true,        // spawn in-process workers
  exclusiveWorkers: true,  // single worker per queue across cluster (default)
});

export async function initQueues() {
  await queues.initQueues({
    connection: new IORedis(process.env.REDIS_URL!),
  });
}
```

2. Define and export a queue using that service:

```
// queues/sum-queue.ts
import { z } from 'zod';
import { queues } from './index';

export const sumQueue = queues.defineQueue({
  name: 'sum',
  inputSchema: z.object({ a: z.number(), b: z.number() }),
  outputSchema: z.object({ sum: z.number() }),
  async process(input) {
    return { sum: input.a + input.b };
  },
});
```

3. Use the `ConcreteQueue` anywhere in your app:

```
// api/route.ts
import { sumQueue } from '../queues/sum-queue';

export async function POST(req) {
  const { a, b } = await req.json();
  const { jobId, awaitResult } = await sumQueue.publish({ a, b });
  const result = await awaitResult(); // fully typed: { sum: number }
  return Response.json({ jobId, result });
}
```

## ConcreteQueue: The Instance You Use

`defineQueue` returns a `ConcreteQueue` with a small set of ergonomic methods:

- `name` → the queue name
- `publish(input, options?) → Promise<AwaitResult<T>>`
- `scheduleAt({ when }, input, options?) → Promise<AwaitResult<T>>`
- `scheduleIn({ delayMs }, input, options?) → Promise<AwaitResult<T>>`
- `scheduleRepeat(spec, input, options?) → Promise<RepeatStream<T>>`

The `AwaitResult<T>` you get back from `publish/schedule*` is:

- `{ jobId: string, awaitResult(options?), cancel() }`
    - `awaitResult({ timeoutMs? })` waits for completion and returns the typed result
    - `cancel()` attempts to remove the job before it runs

Repeat jobs stream each run’s result and can be canceled:

```
const stream = await sumQueue.scheduleRepeat({ type: 'interval', everyMs: 5_000 }, { a: 2, b: 2 });
for await (const { jobId, result } of stream) {
  console.log('repeat', jobId, result);
  if (shouldStop()) await stream.cancel();
}
```

## Context Propagation (runWithContext)

Wrap job processing in your own context function — useful for AsyncLocalStorage, tracing, etc.

```
import { AsyncLocalStorage } from 'node:async_hooks';

const als = new AsyncLocalStorage<Map<string, string>>();

const queues = new QueueService({
  prefix: 'myapp',
  runWorkers: true,
  runWithContext: (fn) => als.run(new Map([['requestId', 'abc-123']]), fn),
});

await queues.initQueues({
  connection: new IORedis(process.env.REDIS_URL!),
});

// Inside your queue `process` function you can read it with als.getStore()
```

## Scheduling and Repeats

Run later at an absolute time:

```
await sumQueue.scheduleAt({ when: new Date(Date.now() + 15_000) }, { a: 1, b: 4 });
```

Run later after a delay:

```
await sumQueue.scheduleIn({ delayMs: 30_000 }, { a: 3, b: 9 });
```

Repeat with results as a stream:

```
const stream = await sumQueue.scheduleRepeat({ type: 'interval', everyMs: 5_000 }, { a: 2, b: 2 });

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

## Common Usage Patterns

- HTTP endpoints: enqueue and await the result in the same request for quick tasks; or return `jobId` and poll elsewhere for longer work.
- Cron-style workers: `await scheduleRepeat({ type: 'cron', cron: '*/5 * * * *' }, payload)` and iterate the stream to act on each run.
- Fire-and-forget: call `sumQueue.publish(payload)` and ignore the returned handle if you don’t need results.
- Strong types: both `input` and `result` are inferred from your Zod schemas.

## Worker Management

- Set `runWorkers: true` in the constructor to spawn in‑process workers.
- `exclusiveWorkers` (default: true) acquires a lightweight Redis lock so only one worker runs per queue across your cluster.
- If you enable late registration via `allowLateRegistration()`, the service can materialize new queues after init.
- Call `reconcileWorkers()` to attempt to spawn workers for runtimes that are missing one (useful if a lock was released and you want to race for it again).

## API Summary

- `class QueueService({ prefix, logger?, runWorkers?, allowLateRegistration?, exclusiveWorkers?, runWithContext? })`
    - `defineQueue({ name, inputSchema, outputSchema?, process, defaults?, hooks?, init? }) → ConcreteQueue`
    - `initQueues({ connection })`
    - `getQueue<Name extends keyof ToreroQueues>(name: Name) → ConcreteQueue<QueueInput<Name>, QueueOutput<Name>>`
    - `materialize(name, { runWorkers? })`
    - `reconcileWorkers()`
    - `allowLateRegistration()`
    - `shutdown()`

## Releases

- We use Changesets for batched releases.
- For any PR that changes behavior, add a changeset:
    - Run: `pnpm dlx changeset`
    - Choose patch/minor/major and write a short note.
- After PRs merge to `main`, a bot opens/updates a "Version Packages" PR.
- Merge that PR to publish to npm and create the GitHub Release.

- `interface ConcreteQueue<TInput, TOutput>`
    - `name: string`
    - `publish(input, options?) → Promise<AwaitResult<TOutput>>`
    - `scheduleAt({ when }, input, options?) → Promise<AwaitResult<TOutput>>`
    - `scheduleIn({ delayMs }, input, options?) → Promise<AwaitResult<TOutput>>`
    - `scheduleRepeat(spec, input, options?) → Promise<RepeatStream<TOutput>>`

Key types:

- `PublishOptions<T>` → attempts, backoff, timeoutMs, priority, idempotencyKey, removeOnComplete/Fail, jobId
- `QueueDefaults` → per‑queue defaults for the above (plus concurrency and optional limiter)
- `RepeatSpec` →
    - Interval: `{ type: 'interval'; everyMs: number; startDate?: Date; endDate?: Date; limit?: number }`
    - Cron: `{ type: 'cron'; cron: string; startDate?: Date; endDate?: Date; limit?: number }`
- `RepeatStream<T>` → async iterable of `{ jobId, result }`; call `cancel()` to unschedule; `close()` to stop listening.
- `AwaitResult<T>` → `{ jobId, awaitResult, cancel }`

## Repeat Scheduling (Job Scheduler)

- Under the hood, repeat jobs use BullMQ’s Job Scheduler API: `upsertJobScheduler` and `removeJobScheduler`.
- `scheduleRepeat(...)` returns a Promise and will reject if the scheduler upsert fails.
- The first repetition for a newly inserted scheduler runs immediately; subsequent runs follow the schedule.
- `cancel()` removes the job scheduler (stopping future runs) and closes the stream; `close()` only stops listening.
- You can provide an `idempotencyKey` in options to derive a stable scheduler ID; otherwise a random ID is used.
- Scheduler‑produced jobs get special IDs; custom `jobId` is not applied to these jobs (BullMQ constraint).
- Supported repeat controls: `startDate`, `endDate`, `limit` on both interval and cron specs.
- `Logger` → `{ info, warn, error }`

## QueueService Options

- `prefix` (required)
    - Namespaces all Redis keys for queues, events, and locks.
    - Choose a short, app-specific string (e.g., `myapp`).
- `logger` (default: no-op logger)
    - Object with `info`, `warn`, `error` functions. Used by workers, events, and internals.
- `runWorkers` (default: false)
    - When true, spawns in-process workers for all registered queues on this instance.
- `exclusiveWorkers` (default: true)
    - Ensures only one worker per queue across your cluster via a small Redis lock.
- `allowLateRegistration` (default: false)
    - Allows calling `defineQueue()` after `initQueues()`. Late definitions are materialized automatically using the current worker settings.
- `runWithContext` (default: undefined)
    - Function wrapper for job execution; useful for AsyncLocalStorage, tracing, or scoping per-job context.
    - Signature: `(fn) => PromiseLike` — called around each job’s `process`.

## Typed getQueue (optional)

Note: we generally discourage `getQueue`. The preferred pattern is to export your `ConcreteQueue` handles and import them where needed. Using `getQueue` requires extra typing work (module augmentation) and often signals an architectural smell (e.g., excessive dynamic routing). Keep it for rare edge cases only.

If you still need dynamic access (e.g., by name in a registry), you can keep strong types by augmenting the library’s registry interface:

```
// global.d.ts (or any .d.ts included by tsconfig)
import type { z } from 'zod';

// Reuse the schemas/types you defined alongside the queue
import type { sumInputSchema, sumOutputSchema } from './queues/sum-queue';

declare module 'torero-mq' {
  interface ToreroQueues {
    sum: {
      input: z.infer<typeof sumInputSchema>;
      output: z.infer<typeof sumOutputSchema>;
    };
  }
}
```

Then `getQueue('sum')` is fully typed:

```
import { queues } from './queues';
const q = queues.getQueue('sum');
// q is ConcreteQueue<{ a: number; b: number }, { sum: number }>
```

## Development

- Build: `pnpm run build`
- Test: `pnpm test`

## License

MIT — see `LICENSE`.
