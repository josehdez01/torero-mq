# torero-mq

## 0.2.0

### Minor Changes

- Breaking rewrite of the 0.1 API. Requires Node.js 26+, BullMQ 6 and ioredis 6 for the default Redis adapter.

    - Replace QueueService and global registration with immutable Standard Schema job contracts, typed nested producer clients, and explicitly started worker groups. Contracts can be imported through `torero-mq/contract` without loading BullMQ.
    - Infer raw input, parsed handler input, validated output, per-job context, and middleware enrichment. Support asynchronous schemas and code-first result inference without module augmentation.
    - Add versioned rich-value serialization, retained typed result handles, native deduplication, bulk publishing, delayed jobs, global concurrency, and stable recurring schedules with bounded result subscriptions.
    - Fix namespace propagation, lifecycle and connection ownership, result-observation races, permanent validation failures, and cooperative execution deadlines that retain the worker slot until cleanup finishes.
    - Refresh dependencies and release tooling; add isolated Redis integration tests and compiler tests against an installed package tarball.

    Old QueueService calls and persisted 0.1 jobs are incompatible. Use a new Redis prefix or drain old queues, including delayed/retrying work, before switching. Recreate schedules deliberately. See `docs/migration.md` in the repository for migration and delivery guarantees.
