import { describe, expect, it } from 'vitest';
import { encode, decode } from '../src/codec.js';
import { job } from '../src/definition.js';
import { CodecError, JobCancelledError, JobError, ValidationError } from '../src/errors.js';
import {
    assertJson,
    closeAll,
    failedError,
    failureReason,
    inputEnvelope,
    outputEnvelope,
    ProtocolError,
    readInput,
    readOutput,
} from '../src/shared.js';

describe('serialization boundaries', () => {
    it.each([
        'not json',
        'null',
        '[]',
        '{}',
        '{"version":1}',
        '{"version":1,"payload":null}',
        '{"version":1,"payload":{}}',
    ])('rejects malformed codec envelopes: %s', (wire) => {
        expect(() => decode(wire)).toThrow(CodecError);
    });
    it('reports decoding failures with their original cause', () => {
        try {
            decode('{');
            throw new Error('Expected decode failure');
        } catch (error) {
            expect(error).toBeInstanceOf(CodecError);
            expect(error).toHaveProperty('cause');
        }
    });
    it('reports serialization traps without leaking raw exceptions from codecs', () => {
        const cause = new Error('object was revoked');
        const unstable = new Proxy(
            {},
            {
                getPrototypeOf() {
                    throw cause;
                },
            },
        );
        try {
            encode(unstable);
            throw new Error('Expected encode failure');
        } catch (error) {
            expect(error).toBeInstanceOf(CodecError);
            expect(error).toHaveProperty('cause', cause);
        }
    });
    it('rejects symbol properties and custom array properties that JSON would erase', () => {
        expect(() => encode({ [Symbol('hidden')]: 1 })).toThrow('Symbol properties');
        expect(() => encode(Object.assign([1], { label: 'lost' }))).toThrow(
            'Custom array properties',
        );
        expect(() => encode(new Uint8Array([1, 2]))).toThrow(CodecError);
    });
    it('preserves versioned undefined results and rich input values', () => {
        const contract = job('wire', { version: 3 });
        const value = { at: new Date('2026-10-19'), amount: 99n };
        const wire = inputEnvelope(contract, value, 'scheduler');
        expect(wire).toMatchObject({
            torero: 1,
            name: 'wire',
            version: 3,
            schedulerId: 'scheduler',
        });
        expect(readInput(contract, wire)).toEqual(value);
        expect(readOutput(contract, outputEnvelope(contract, undefined))).toBeUndefined();
    });
    it('rejects another job or schema revision instead of accepting its encoded value', () => {
        const contract = job('wire', { version: 2 });
        for (const wire of [
            null,
            [],
            { torero: 1, name: 'other', version: 2, input: encode(1) },
            { torero: 1, name: 'wire', version: 1, input: encode(1) },
            { torero: 2, name: 'wire', version: 2, input: encode(1) },
        ])
            expect(() => readInput(contract, wire)).toThrow(ProtocolError);
        expect(() =>
            readInput(contract, { torero: 1, name: 'wire', version: 2, input: 1 }),
        ).toThrow('Missing encoded input');
        expect(() =>
            readOutput(contract, { torero: 1, name: 'wire', version: 2, output: null }),
        ).toThrow('Missing encoded output');
    });
});

describe('failure transport', () => {
    it('preserves domain data and messages while keeping local causes off the wire', () => {
        const cause = new Error('private database credentials');
        const data = { retryAt: new Date('2026-10-19'), amount: 7n, missing: undefined };
        const error = new JobError('PAYMENT_PENDING', data, {
            message: 'Please retry later',
            retryable: true,
            cause,
        });
        expect(error.cause).toBe(cause);
        expect(error.retryable).toBe(true);
        const wire = failureReason(error);
        expect(wire).not.toContain('private database credentials');
        expect(failedError('billing', 'job-1', wire)).toMatchObject({
            name: 'JobFailedError',
            queue: 'billing',
            jobId: 'job-1',
            reason: 'Please retry later',
            details: { code: 'PAYMENT_PENDING', data },
        });
        expect(new JobError('PERMANENT', null)).toMatchObject({
            message: 'PERMANENT',
            retryable: false,
        });
    });
    it('preserves schema issue paths across worker failure boundaries', () => {
        const error = new ValidationError(
            'output',
            [{ message: 'Invalid recipient', path: ['guests', { key: 2 }, 'email'] }],
            'mail',
        );
        expect(failedError('mail', 'job-2', failureReason(error))).toMatchObject({
            details: { code: 'VALIDATION_ERROR', phase: 'output', issues: error.issues },
        });
    });
    it('retains the primary failure when details cannot be serialized', () => {
        const error = new JobError(
            'BAD_DETAILS',
            { callback: () => 1 },
            { message: 'Primary failure' },
        );
        expect(failedError('mail', 'job-3', failureReason(error))).toMatchObject({
            reason: 'Primary failure',
            details: {
                code: 'BAD_DETAILS',
                serializationError: 'Error details could not be serialized',
            },
        });
    });
    it.each([
        'plain BullMQ failure',
        '{"unrelated":true}',
        '{"torero":1,"message":"broken","data":"not a codec envelope"}',
    ])('keeps third-party or corrupt errors readable: %s', (reason) => {
        expect(failedError('mail', 'job-4', reason)).toMatchObject({ reason, details: undefined });
    });
    it('normalizes non-Error throws and represents cancellation consistently', () => {
        expect(failedError('mail', 'job-5', failureReason('plain throw'))).toMatchObject({
            reason: 'plain throw',
            details: { code: 'PROCESSOR_ERROR' },
        });
        expect(failedError('mail', 'job-5', failureReason(null))).toMatchObject({ reason: 'null' });
        expect(
            failedError('mail', 'job-5', failureReason(new JobCancelledError('mail', 'job-5'))),
        ).toMatchObject({ details: { code: 'JOB_CANCELLED' } });
    });
    it('attempts every resource cleanup and reports all failures together', async () => {
        const failures = [new Error('connection one'), new Error('connection two')];
        const closed: number[] = [];
        const close = closeAll(
            [0, 1, 2].map((index) => ({
                async close() {
                    closed.push(index);
                    if (index !== 1) throw failures[index === 0 ? 0 : 1];
                },
            })),
        );
        await expect(close).rejects.toBeInstanceOf(AggregateError);
        await expect(close).rejects.toHaveProperty('errors', failures);
        expect(closed).toEqual([0, 1, 2]);
    });
});

describe('progress JSON boundary', () => {
    it('allows repeated references without mistaking them for cycles', () => {
        const shared = { value: 1 };
        expect(() =>
            assertJson({ first: shared, second: shared, scalars: [null, true, 'done', 1] }),
        ).not.toThrow();
        expect(() =>
            assertJson(Object.assign(Object.create(null) as object, { percent: 50 })),
        ).not.toThrow();
    });
    it.each([
        undefined,
        NaN,
        Infinity,
        1n,
        () => 1,
        Symbol('value'),
        new Date(),
        new Map(),
        Array(2),
        { [Symbol('key')]: 1 },
    ])('rejects progress that cannot roundtrip as JSON: %s', (value) => {
        expect(() => assertJson(value)).toThrow(CodecError);
    });
    it('rejects cycles, hidden state and accessors without invoking user code', () => {
        const cycle: { self?: unknown } = {};
        cycle.self = cycle;
        let invoked = false;
        const accessor = {
            get value() {
                invoked = true;
                return 1;
            },
        };
        const hidden = Object.defineProperty({}, 'value', { value: 1 });
        // Keep key count equal to array length so property validation must reject the custom key.
        const custom = Object.assign(Array(1), { label: 'lost' });
        for (const value of [cycle, accessor, hidden, custom])
            expect(() => assertJson(value)).toThrow(CodecError);
        expect(invoked).toBe(false);
    });
});
