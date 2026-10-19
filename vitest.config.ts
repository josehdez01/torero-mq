import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        globals: true,
        environment: 'node',
        include: ['tests/**/*.test.ts'],
        exclude: process.env.TORERO_REDIS_TEST ? [] : ['tests/integration/**'],
        testTimeout: 15_000,
        hookTimeout: 15_000,
        coverage: {
            provider: 'v8',
            include: ['src/**/*.ts'],
            thresholds: { statements: 98, branches: 95, functions: 95, lines: 99 },
        },
    },
});
