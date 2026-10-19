# torero-mq

Typed job contracts, producers and workers for BullMQ. Define jobs once, share contracts with producers, and attach handlers in your worker process. Nested routers preserve input and result types without module augmentation.

Version 0.2 replaces the 0.1 API and wire format. Read [the migration guide](docs/migration.md) before switching an existing deployment.

## Install

Requires Node.js **26 or newer**, BullMQ **6**, and Redis. The default Redis driver is ioredis **6**. Schemas use [Standard Schema](https://standardschema.dev/); Zod is an example, not a Torero dependency.

```sh
pnpm add torero-mq bullmq@^6.3.17 ioredis@^6.0.0 zod
```

## Define, publish, process

```ts
import { z } from 'zod';
import { job, createQueueClient, createWorkers } from 'torero-mq';

const sum = job('math.sum', { version: 1 })
    .input(z.object({ a: z.number(), b: z.number() }))
    .output(z.object({ total: z.number() }))
    .options({ attempts: 3, backoff: { type: 'exponential', delay: 500 } });

const settings = {
    connection: { url: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379' },
    prefix: 'my-app',
};
const client = createQueueClient({ math: { sum } }, settings);
const workers = createWorkers(
    {
        math: {
            sum: sum.handler(({ input }) => ({ total: input.a + input.b })),
        },
    },
    settings,
);

try {
    await workers.start();
    const handle = await client.jobs.math.sum.publish({ a: 2, b: 3 });
    const result = await handle.wait({ timeoutMs: 5_000 }); // { total: number }
    console.log(handle.id, result.total);
} finally {
    await workers.close();
    await client.close();
}
```

Definitions never connect to Redis. Producers start lazily on their first operation; `await client.start()` connects explicitly. Workers process only after `await workers.start()`. Producers and workers can run in separate processes using the same contract, prefix and Redis connection.

Put shared contracts in a module importing `job` from `torero-mq/contract`. This entry point loads no BullMQ or Redis runtime. Keep handlers and infrastructure imports in worker modules. Job names are explicit and stable: moving `math.sum` inside a different router path does not rename its queue. Names must be unique within a router; contract versions identify compatible wire payloads.

## Job controls

```ts
const queue = client.jobs.math.sum;
const handle = queue.get('existing-id'); // synchronous typed reference
await handle.status(); // BullMQ state, or 'unknown'
await handle.result(); // saved result; throws if pending/failed/missing
await handle.wait({ signal: AbortSignal.timeout(5_000) });
await handle.retry(); // retry a failed job
await handle.remove(); // 'removed' | 'missing' | 'active'

await queue.publishBulk([
    { input: { a: 1, b: 2 }, options: { jobId: 'invoice-123' } },
    { input: { a: 3, b: 4 } },
]);
await queue.schedule({ a: 1, b: 2 }, { delayMs: 10_000 });
await queue.schedule({ a: 1, b: 2 }, { at: new Date(Date.now() + 10_000) });
await queue.counts();
await queue.pause();
await queue.resume();
await queue.setGlobalConcurrency(2);
await queue.removeGlobalConcurrency();
const raw = await queue.raw(); // BullMQ administration; payloads are Torero envelopes
```

Publish options include `jobId`, `delay`, native `deduplication`, `attempts`, `backoff`, `priority`, `lifo` and retention options. `attempts` includes the first run. Custom job IDs must be nonempty, contain no `:` and not consist entirely of digits. Bulk publishing does not transact with your application database.

## Recurring jobs

```ts
const schedule = client.jobs.math.sum.schedules.get('daily-sum');
const events = await schedule.subscribe({ capacity: 100 });
await client.jobs.math.sum.schedules.upsert(
    'daily-sum',
    { cron: '0 9 * * *', timezone: 'America/Mexico_City' },
    { a: 2, b: 3 },
);
try {
    for await (const event of events) {
        if (event.type === 'completed') console.log(event.result.total);
        else console.error(event.error);
        break;
    }
} finally {
    events.close(); // stop this observer
    await schedule.remove(); // stop future scheduled runs
}
```

Use `{ everyMs: 60_000 }` for intervals. Both schedule forms accept `startDate`, `endDate` and `limit`. `schedules.list()` returns public IDs and schedule metadata. Subscribe **before** upsert to observe an immediate first run. Subscriptions are live, future-only, best-effort streams; they are not a durable audit log. Their buffer is bounded and throws `StreamOverflowError` on overflow. Closing an iterator does not remove its scheduler.

## Guarantees and boundaries

- Inputs are encoded into an immutable snapshot before enqueue validation. Each worker attempt validates and transforms that original input; transforms do not accumulate across retries. Validators should be pure because they may run at enqueue and on each attempt. Async schemas are supported; output schemas validate and transform handler results.
- The codec preserves `Date`, `bigint`, `undefined`, `Map`, `Set`, `RegExp`, special numbers and plain nested values. It rejects functions, symbols and custom class instances.
- Completed jobs default to retention of 24 hours / 1,000 jobs; failed jobs to 7 days / 5,000 jobs. These are best-effort BullMQ limits with lazy eviction. Late waits and restarted clients can read retained results. `removeOnComplete: true` forfeits that ability.
- Wait timeouts and abort signals affect only the waiter. Removing an active job returns `'active'` and does not stop its handler.
- Worker `timeoutMs` aborts the handler signal, then waits for the handler to settle before releasing its lock and slot or retrying. A handler that ignores cancellation can keep running and hold graceful `close()` indefinitely. It is a cooperative deadline, not a hard process kill. Worker, wait and startup timeouts must be positive integer milliseconds no greater than 2,147,483,647.
- BullMQ provides at-least-once execution. Use application idempotency, provider deduplication and a transactional outbox where needed.
- `JobError(code, data)` is permanent by default; `{ retryable: true }` opts into retries. Failures surface as `JobFailedError` with persisted code/data in `details`. Promise rejection types are not inferred from contracts.
- Plain connection options create owned connections. Supplied Redis clients remain caller-owned and are never quit by Torero; borrowed worker clients need `maxRetriesPerRequest: null`. Close every client, worker group and subscription you create.

See [application patterns](docs/project-patterns.md), [migration](docs/migration.md), and the [runnable example](examples/sum-queue.ts).

## Development

```sh
pnpm install --frozen-lockfile
pnpm typecheck
pnpm test
pnpm test:integration # creates and removes its own Docker Redis
pnpm test:coverage    # all unit + Redis tests, with combined coverage
pnpm test:package     # builds, packs, installs, imports and typechecks a real consumer
```

MIT.
