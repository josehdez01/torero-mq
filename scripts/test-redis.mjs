import { spawn, spawnSync } from 'node:child_process';

let child;
let stopping = false;
function stopChild(signal) {
    if (!child || child.exitCode !== null) return;
    try {
        if (process.platform === 'win32') child.kill(signal);
        else process.kill(-child.pid, signal);
    } catch (error) {
        if (error.code !== 'ESRCH') throw error;
    }
}
function cleanup() {
    spawnSync('docker', ['rm', '--force', name], { stdio: 'ignore', timeout: 15_000 });
}
for (const signal of ['SIGINT', 'SIGTERM'])
    process.on(signal, () => {
        if (stopping) return;
        stopping = true;
        stopChild('SIGTERM');
        cleanup();
        process.exit(signal === 'SIGINT' ? 130 : 143);
    });
const name = `torero-mq-test-${process.pid}-${Date.now()}`;
function docker(...args) {
    const result = spawnSync('docker', args, { encoding: 'utf8', timeout: 120_000 });
    if (result.status !== 0)
        throw new Error(result.stderr || 'Docker is required for Redis integration tests.');
    return result.stdout.trim();
}
try {
    docker(
        'run',
        '--detach',
        '--rm',
        '--name',
        name,
        '--publish',
        '127.0.0.1::6379',
        'redis:8.2-alpine',
    );
    let ready = false;
    for (let attempt = 0; attempt < 60; attempt++) {
        const ping = spawnSync('docker', ['exec', name, 'redis-cli', 'ping'], { encoding: 'utf8' });
        if (ping.status === 0 && ping.stdout.trim() === 'PONG') {
            ready = true;
            break;
        }
        await new Promise((resolve) => setTimeout(resolve, 250));
    }
    if (!ready) throw new Error('Isolated Redis did not become ready.');
    const address = docker('port', name, '6379/tcp');
    const port = address.split(':').at(-1);
    child = spawn(
        'pnpm',
        [
            'exec',
            'vitest',
            'run',
            ...(process.argv.includes('--all') ? [] : ['tests/integration']),
            ...process.argv.slice(2).filter((argument) => argument !== '--all'),
        ],
        {
            stdio: 'inherit',
            detached: process.platform !== 'win32',
            env: { ...process.env, REDIS_URL: `redis://127.0.0.1:${port}`, TORERO_REDIS_TEST: '1' },
        },
    );
    const timeout = setTimeout(() => stopChild('SIGTERM'), 120_000);
    const forceTimeout = setTimeout(() => stopChild('SIGKILL'), 130_000);
    try {
        process.exitCode = await new Promise((resolve, reject) => {
            child.once('error', reject);
            child.once('exit', (code) => resolve(code ?? 1));
        });
    } finally {
        clearTimeout(timeout);
        clearTimeout(forceTimeout);
    }
} finally {
    cleanup();
}
