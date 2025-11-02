import { describe, it, expect } from 'vitest';
import { deriveJobId, hashString, toBullOptions } from './helpers.js';

describe('helpers', () => {
    it('hashString is deterministic and SHA1 length', () => {
        const a = hashString('hello');
        const b = hashString('hello');
        const c = hashString('world');
        expect(a).toBe(b);
        expect(a).not.toBe(c);
        // sha1 hex length
        expect(a).toHaveLength(40);
        expect(/^[0-9a-f]+$/.test(a)).toBe(true);
    });

    it('deriveJobId includes prefix, queue and hash', () => {
        const id = deriveJobId('pref', 'math', 'a:1');
        expect(id.startsWith('pref:math:')).toBe(true);
        // ensure stable for same inputs
        expect(id).toBe(deriveJobId('pref', 'math', 'a:1'));
        // different key -> different id
        expect(id).not.toBe(deriveJobId('pref', 'math', 'a:2'));
    });

    it('toBullOptions maps options and defaults correctly', () => {
        const opts1 = toBullOptions(undefined, undefined);
        expect(opts1.removeOnComplete).toBe(true);
        expect(opts1.removeOnFail).toBe(false);

        const opts2 = toBullOptions(
            { attempts: 3, backoff: { type: 'fixed', delay: 100 }, removeOnFail: 10 },
            { retries: 5, priority: 2, removeOnComplete: false },
        );
        expect(opts2.attempts).toBe(5); // overrides defaults
        expect(opts2.backoff).toEqual({ type: 'fixed', delay: 100 });
        expect(opts2.priority).toBe(2);
        expect(opts2.removeOnComplete).toBe(false);
        expect(opts2.removeOnFail).toBe(10);

        const opts3 = toBullOptions(
            { removeOnComplete: 50, removeOnFail: 0 },
            { removeOnComplete: true, removeOnFail: true },
        );
        expect(opts3.removeOnComplete).toBe(true);
        expect(opts3.removeOnFail).toBe(true);
    });
});
