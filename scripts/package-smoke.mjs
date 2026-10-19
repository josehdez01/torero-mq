import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const directory = await mkdtemp(join(tmpdir(), 'torero-mq-package-'));
function run(command, args, cwd) {
    const result = spawnSync(command, args, { cwd, encoding: 'utf8', timeout: 120_000 });
    assert.equal(result.status, 0, result.stderr || result.stdout);
    return result.stdout;
}
try {
    const packed = JSON.parse(
        run('npm', ['pack', '--json', '--pack-destination', directory], process.cwd()),
    );
    const archive = join(directory, packed[0].filename);
    assert(packed[0].files.some((file) => file.path === 'dist/index.js'));
    assert(packed[0].files.some((file) => file.path === 'dist/index.d.ts'));
    assert(packed[0].files.some((file) => file.path === 'src/index.ts'));
    assert(packed[0].files.some((file) => file.path === 'dist/contract.d.ts'));
    assert(!packed[0].files.some((file) => /(?:test|node_modules)/.test(file.path)));
    const consumer = join(directory, 'consumer');
    await mkdir(consumer);
    const manifest = JSON.parse(await readFile('package.json', 'utf8'));
    await writeFile(
        join(consumer, 'package.json'),
        JSON.stringify({
            private: true,
            type: 'module',
            dependencies: {
                'torero-mq': `file:${archive}`,
                ...manifest.peerDependencies,
                zod: manifest.devDependencies.zod,
                typescript: manifest.devDependencies.typescript,
                '@types/node': manifest.devDependencies['@types/node'],
            },
        }),
    );
    run('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund'], consumer);
    run(
        process.execPath,
        [
            '--input-type=module',
            '-e',
            'const api = await import("torero-mq"); for (const key of ["job", "implement", "createQueueClient", "createWorkers", "JobError"]) if (typeof api[key] !== "function") throw new Error(`Missing public export: ${key}`); const contract = await import("torero-mq/contract"); if (typeof contract.job !== "function" || typeof contract.implement !== "function") throw new Error("Missing contract exports");',
        ],
        consumer,
    );
    await writeFile(
        join(consumer, 'tsconfig.json'),
        JSON.stringify({
            compilerOptions: {
                target: 'ESNext',
                module: 'NodeNext',
                moduleResolution: 'NodeNext',
                strict: true,
                noEmit: true,
                skipLibCheck: false,
                types: ['node'],
            },
            include: ['consumer.ts'],
        }),
    );
    await writeFile(
        join(consumer, 'consumer.ts'),
        `
import { z } from 'zod';
import { job, implement } from 'torero-mq/contract';
import type { InferInputs, InferOutputs } from 'torero-mq/contract';
import { createQueueClient, createWorkers } from 'torero-mq';

const transformed = job('consumer.transform')
    .input(z.object({ amount: z.string().transform(Number), label: z.string().default('default') }))
    .output(z.number().transform((value) => ({ total: String(value) })));
const implementation = implement(transformed).handler(({ input }) => {
    const amount: number = input.amount;
    const label: string = input.label;
    void label;
    // @ts-expect-error worker input has already been transformed
    const raw: string = input.amount;
    void raw;
    return amount * 2;
});
const router = { billing: { transformed } };
type Inputs = InferInputs<typeof router>;
type Outputs = InferOutputs<typeof router>;
const input: Inputs['billing']['transformed'] = { amount: '4' };
const output: Outputs['billing']['transformed'] = { total: '8' };
void output;
const settings = { connection: { host: '127.0.0.1' }, prefix: 'consumer-typecheck' };
const client = createQueueClient(router, settings);
createWorkers({ billing: { transformed: implementation } }, settings);
async function exercise() {
    const handle = await client.jobs.billing.transformed.publish(input);
    const result = await handle.wait();
    const total: string = result.total;
    // @ts-expect-error transformed output is a string field
    const numeric: number = result.total;
    // @ts-expect-error publish accepts raw schema input, not transformed input
    await client.jobs.billing.transformed.publish({ amount: 4 });
    // @ts-expect-error required raw input cannot be omitted
    await client.jobs.billing.transformed.publish();
    // @ts-expect-error nested router preserves property names
    client.jobs.billing.missing;
    await client.jobs.billing.transformed.publishBulk([{ input: { amount: '5' } }]);
    const persisted: { total: string } = await client.jobs.billing.transformed.get(handle.id).result();
    void [total, numeric, persisted];
}
void exercise;
`,
    );
    run(process.execPath, ['node_modules/typescript/bin/tsc', '-p', 'tsconfig.json'], consumer);
    console.log('Packed package installs, imports, and typechecks a real consumer successfully.');
} finally {
    await rm(directory, { recursive: true, force: true });
}
