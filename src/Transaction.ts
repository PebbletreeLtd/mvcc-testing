import {
    TOMBSTONE,
    type Tombstone,
    type ReadOperation,
    type VersionedEntry,
} from "./types";

/**
 * Represents a single MVCC transaction operating on a point-in-time snapshot.
 *
 * All reads observe the store as of `readVersion`.  Writes are buffered
 * locally and only applied to the store during the commit phase (handled by
 * `MVCCStore`).
 *
 * Inspired by FoundationDB's `Transaction` — callers interact with `get`,
 * `set`, `clear`, and `getUsingFilter` inside a `doTransaction` callback.
 *
 * @typeParam K - The key type (any JSON-serialisable object).
 * @typeParam V - The value type stored under each key.
 */
export class Transaction<K, V> {
    /**
     * All read operations performed during this transaction, recorded as a
     * discriminated union so the commit phase can apply the appropriate
     * conflict check for each kind.
     */
    private readonly _readOperations: ReadOperation<K, V>[] = [];

    /**
     * Buffered writes (including clears stored as `TOMBSTONE`).
     * Applied to the store atomically at commit time.
     */
    private readonly _writeBuffer = new Map<string, V | Tombstone>();

    constructor(
        /** The snapshot version this transaction reads from. */
        private readonly readVersion: number,
        /** Reference to the store's internal versioned data. */
        private readonly versionMap: Map<string, VersionedEntry<V>[]>,
        /** Deterministic key serialiser (json-stable-stringify). */
        private readonly serialize: (key: K) => string,
        /** Deserialise a serialised key string back to the key object. */
        private readonly deserialize: (serialised: string) => K,
        /**
         * When `true`, reads consult the local write buffer before the store.
         * When `false`, reads always go straight to the versioned snapshot.
         */
        private readonly ryow: boolean = true,
    ) { }

    // ---------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------

    /**
     * Read the value for `key` as of this transaction's snapshot.
     *
     * A `read` operation is recorded so that conflicts can be detected at
     * commit time.
     *
     * Resolution order:
     *  1. Local write buffer (uncommitted writes within this txn).
     *  2. Version history in the store (latest entry ≤ readVersion).
     *
     * Returns `undefined` when the key does not exist or has been cleared.
     */
    get(key: K): V | undefined {
        const serialised = this.serialize(key);
        this._readOperations.push({ type: "read", key: serialised });

        // 1. Check local write buffer first (if RYOW is enabled).
        if (this.ryow && this._writeBuffer.has(serialised)) {
            const buffered = this._writeBuffer.get(serialised);
            return buffered === TOMBSTONE ? undefined : (buffered as V);
        }

        // 2. Fall back to the versioned store.
        return this.readFromStore(serialised);
    }

    /**
     * Scan every key in the store, passing each live key/value pair to
     * `filter`.  Returns all entries for which the filter returns `true`.
     *
     * Two kinds of conflict tracking are recorded:
     *  1. A `read` operation for each *matched* key — so any value change on
     *     a matched row is detected the same way as a point-read conflict.
     *  2. A single `filterRead` operation that captures the filter callback
     *     and the set of matched serialised keys.  At commit time the store
     *     re-runs the filter to detect keys that were added to or removed
     *     from the result set by concurrent transactions.
     */
    getUsingFilter(
        filter: (key: K, value: V) => boolean,
    ): [K, V][] {
        const results: [K, V][] = [];
        const matchedKeys = new Set<string>();

        // Iterate every known serialised key in the store.
        for (const serialisedKey of this.versionMap.keys()) {
            // Resolve the current visible value (write-buffer then store).
            const value = this.resolveValue(serialisedKey);
            if (value === undefined) {
                continue; // key does not exist or is tombstoned at this snapshot
            }

            const key = this.deserialize(serialisedKey);
            if (filter(key, value)) {
                matchedKeys.add(serialisedKey);
                results.push([key, value]);

                // Record an individual read for conflict detection on value changes.
                this._readOperations.push({ type: "read", key: serialisedKey });
            }
        }

        // Also check keys that only exist in the local write buffer (newly set
        // within this transaction but not yet in the store).
        // These are NOT added to matchedKeys — they are local to this transaction
        // and the filter-conflict check re-runs against the committed store, so
        // including them would cause false positives.
        for (const [serialisedKey, buffered] of this._writeBuffer) {
            if (matchedKeys.has(serialisedKey)) {
                continue; // already evaluated above
            }
            if (buffered === TOMBSTONE) {
                continue;
            }
            const key = this.deserialize(serialisedKey);
            if (filter(key, buffered as V)) {
                results.push([key, buffered as V]);
                // No individual read recorded here — these are uncommitted local
                // writes, not store reads.
            }
        }

        // Record the filter-read operation for set-membership conflict detection.
        this._readOperations.push({ type: "filterRead", filter, matchedKeys });

        return results;
    }

    /**
     * Buffer a write of `value` for `key`.
     * The write is not visible to other transactions until commit.
     */
    set(key: K, value: V): void {
        const serialised = this.serialize(key);
        this._writeBuffer.set(serialised, value);
    }

    /**
     * Mark `key` as deleted.  After commit the key will read as `undefined`.
     */
    clear(key: K): void {
        const serialised = this.serialize(key);
        this._writeBuffer.set(serialised, TOMBSTONE);
    }

    // ---------------------------------------------------------------------------
    // Internal — exposed for the commit phase in MVCCStore
    // ---------------------------------------------------------------------------

    /** @internal */
    get readOperations(): ReadonlyArray<ReadOperation<K, V>> {
        return this._readOperations;
    }

    /** @internal */
    get writeBuffer(): ReadonlyMap<string, V | Tombstone> {
        return this._writeBuffer;
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    /**
     * Resolve the visible value for a serialised key, checking the local write
     * buffer first, then falling back to the versioned store.
     * Returns `undefined` if the key is absent or tombstoned.
     */
    private resolveValue(serialisedKey: string): V | undefined {
        if (this.ryow && this._writeBuffer.has(serialisedKey)) {
            const buffered = this._writeBuffer.get(serialisedKey);
            return buffered === TOMBSTONE ? undefined : (buffered as V);
        }
        return this.readFromStore(serialisedKey);
    }

    /**
     * Scan the version array for `serialisedKey` and return the latest value
     * whose version is ≤ `this.readVersion`, or `undefined` if none exists
     * (or the latest visible entry is a tombstone).
     *
     * The version array is kept in ascending version order (append-only), so
     * we scan backwards from the end for efficiency.
     */
    private readFromStore(serialisedKey: string): V | undefined {
        const entries = this.versionMap.get(serialisedKey);
        if (!entries || entries.length === 0) {
            return undefined;
        }

        // Walk backwards to find the newest entry visible at readVersion.
        for (let i = entries.length - 1; i >= 0; i--) {
            const entry = entries[i]!;
            if (entry.version <= this.readVersion) {
                return entry.value === TOMBSTONE ? undefined : entry.value;
            }
        }

        return undefined;
    }
}
