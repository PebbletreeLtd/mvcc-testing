import { OrderedMap } from "./OrderedMap";
import {
    TOMBSTONE,
    type Tombstone,
    type ReadOperation,
    type VersionedEntry,
    type RangeOptions,
    type ITransaction,
    type ISubspace,
} from "./types";

/**
 * Increment the last byte of a hex-encoded key string to produce an exclusive
 * upper bound — equivalent to FoundationDB's `strinc`.
 *
 * Works on the raw bytes: strips trailing `ff` bytes, then increments the
 * last non-`ff` byte.  If all bytes are `ff`, returns a hex string one byte
 * longer (`ff` + 1 → `00` with carry) which is lexicographically greater
 * than any key with the same length prefix.
 */
function strincHex(hex: string): string {
    // Convert hex pairs to a byte array.
    const bytes: number[] = [];
    for (let i = 0; i < hex.length; i += 2) {
        bytes.push(parseInt(hex.slice(i, i + 2), 16));
    }

    // Walk backwards, stripping 0xff bytes.
    while (bytes.length > 0 && bytes[bytes.length - 1] === 0xff) {
        bytes.pop();
    }

    if (bytes.length === 0) {
        // All bytes were 0xff — return a key that sorts after everything
        // with the same prefix length.  In practice this is an edge case.
        return hex + "00";
    }

    bytes[bytes.length - 1]!++;
    return bytes.map((b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Represents a single MVCC transaction operating on a point-in-time snapshot.
 *
 * All reads observe the store as of `readVersion`.  Writes are buffered
 * locally and only applied to the store during the commit phase (handled by
 * `Store`).
 *
 * Inspired by FoundationDB's `Transaction` — callers interact with `get`,
 * `set`, and `clear` inside a `doTransaction` callback.
 *
 * @typeParam K - The key type (any JSON-serialisable object).
 * @typeParam V - The value type stored under each key.
 */
export class Transaction<Kin, KOut, Vin, VOut> implements ITransaction<Kin, KOut, Vin, VOut> {
    /**
     * All read operations performed during this transaction, recorded as a
     * discriminated union so the commit phase can apply the appropriate
     * conflict check for each kind.
     */
    private readonly _readOperations: ReadOperation[];

    /**
     * Buffered writes (including clears stored as `TOMBSTONE`).
     * Applied to the store atomically at commit time.
     */
    private readonly _writeBuffer: Map<string, string | Buffer | Tombstone>;
    /** Reference to the store's internal versioned data. */
    private readonly versionMap: OrderedMap<string, VersionedEntry[]>;

    constructor(
        /** The snapshot version this transaction reads from. */
        private readonly readVersion: number,

        readonly subspace: ISubspace<Kin, KOut, Vin, VOut>,
        /**
         * When `true`, reads consult the local write buffer before the store.
         * When `false`, reads always go straight to the versioned snapshot.
         */
        private readonly ryow: boolean = true,
        /**
         * Optional shared state — when provided (by `at()`), this transaction
         * shares read-operations, write-buffer, and versionMap with the parent
         * so that all scoped operations participate in the same commit.
         */
        sharedReadOps?: ReadOperation[],
        sharedWriteBuffer?: Map<string, string | Buffer | Tombstone>,
        sharedVersionMap?: OrderedMap<string, VersionedEntry[]>,
    ) {
        this._readOperations = sharedReadOps ?? [];
        this._writeBuffer = sharedWriteBuffer ?? new Map();

        if (sharedVersionMap) {
            // Scoped transaction — use the parent's versionMap.
            // If the subspace has its own versionMap that differs, that's
            // a cross-store error.
            if (subspace.versionMap && subspace.versionMap !== sharedVersionMap) {
                throw new Error("at() requires a subspace backed by the same store");
            }
            this.versionMap = sharedVersionMap;
        } else {
            if (!subspace.versionMap) {
                throw new Error("Subspace must have a reference to the version map");
            }
            this.versionMap = subspace.versionMap;
        }
    }


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
    get(key: Kin): VOut | undefined {
        const serialised = this.serialiseKey(key);
        this._readOperations.push({ type: "read", key: serialised });

        // 1. Check local write buffer first (if RYOW is enabled).
        if (this.ryow && this._writeBuffer.has(serialised)) {
            const buffered = this._writeBuffer.get(serialised);
            return (buffered === undefined || buffered === TOMBSTONE)
                ? undefined
                : this.subspace.unpackValue(Buffer.from(buffered));
        }

        // 2. Fall back to the versioned store.
        return this.readFromStore(serialised);
    }



    /**
     * Return all key/value pairs whose serialised key falls within
     * `[start, end)` — i.e. `key >= start` and `key < end` — in sorted
     * order.  Results from the committed store and the local write buffer
     * are merged together.
     *
     * A `scanRead` conflict-check is recorded so that any membership changes
     * in the range between snapshot time and commit time will be detected.
     */
    getRangeAll(start: Kin, end: Kin, opts?: RangeOptions): [KOut, VOut][] {
        return this._getRangeHex(
            this.serialiseKey(start),
            this.serialiseKey(end),
            opts,
        );
    }

    /**
     * Return all key/value pairs whose packed key begins with `prefix`,
     * matching FoundationDB's `getRangeStartsWith` semantics.
     *
     * Internally computes the exclusive upper bound by incrementing the last
     * byte of the prefix buffer (strinc), then delegates to the same range
     * scan used by `getRangeAll`.
     */
    getRangeAllStartsWith(prefix: Kin, opts?: RangeOptions): [KOut, VOut][] {
        const startHex = this.serialiseKey(prefix);
        const endHex = strincHex(startHex);
        return this._getRangeHex(startHex, endHex, opts);
    }

    /**
     * Async generator form of `getRangeAll`.  Yields `[KOut, VOut]` pairs one
     * at a time for the half-open range `[start, end)`.  Conflict tracking is
     * identical to `getRangeAll`.
     */
    async *getRange(start: Kin, end: Kin, opts?: RangeOptions): AsyncGenerator<[KOut, VOut]> {
        const results = this.getRangeAll(start, end, opts);
        for (const entry of results) {
            yield entry;
        }
    }

    /**
     * Async generator form of `getRangeAllStartsWith`.  Yields `[KOut, VOut]`
     * pairs one at a time for all keys matching `prefix`.  Conflict tracking
     * is identical to `getRangeAllStartsWith`.
     */
    async *getRangeStartsWith(prefix: Kin, opts?: RangeOptions): AsyncGenerator<[KOut, VOut]> {
        const results = this.getRangeAllStartsWith(prefix, opts);
        for (const entry of results) {
            yield entry;
        }
    }

    /**
     * Shared implementation for `getRangeAll` and `getRangeAllStartsWith`.
     * Operates on pre-computed hex-encoded key bounds.
     */
    private _getRangeHex(
        startHex: string,
        endHex: string,
        opts?: RangeOptions,
    ): [KOut, VOut][] {
        const matchedKeys = new Set<string>();

        // 1. Query the OrderedMap range — already in sorted order.
        const storeResults: [string, KOut, VOut][] = [];
        const rangeEntries = this.versionMap.getRange(startHex, endHex);
        for (const [serialisedKey] of rangeEntries) {
            const value = this.resolveValue(serialisedKey);
            if (value === undefined) continue;
            matchedKeys.add(serialisedKey);
            storeResults.push([serialisedKey, this.deserializeKey(serialisedKey), value]);
            this._readOperations.push({ type: "read", key: serialisedKey });
        }

        // 2. Check write-buffer-only keys that fall in range.
        const bufferResults: [string, KOut, VOut][] = [];
        for (const [serialisedKey, buffered] of this._writeBuffer) {
            if (matchedKeys.has(serialisedKey)) continue;
            if (serialisedKey < startHex || serialisedKey >= endHex) continue;
            if (buffered === TOMBSTONE) continue;
            const value = this.subspace.unpackValue(Buffer.from(buffered));
            bufferResults.push([serialisedKey, this.deserializeKey(serialisedKey), value]);
        }

        // 3. Merge into sorted order, then apply reverse / limit.
        let results = this.mergeSorted(storeResults, bufferResults);
        if (opts?.reverse) results.reverse();
        if (opts?.limit != null && opts.limit < results.length) {
            results = results.slice(0, opts.limit);
        }

        // 4. Record a scanRead for conflict detection.
        this._readOperations.push({
            type: "scanRead",
            matchedKeys,
            recheck(versionMap) {
                const keys = new Set<string>();
                for (const [sk] of versionMap.getRange(startHex, endHex)) {
                    const entries = versionMap.get(sk);
                    if (!entries || entries.length === 0) continue;
                    const latest = entries[entries.length - 1]!;
                    if (latest.value === TOMBSTONE) continue;
                    keys.add(sk);
                }
                return keys;
            },
        });

        return results;
    }

    /**
     * Buffer a write of `value` for `key`.
     * The write is not visible to other transactions until commit.
     */
    set(key: Kin, value: Vin): void {
        const serialised = this.serialiseKey(key);
        this._writeBuffer.set(serialised, this.subspace.packValue(value));
    }

    /**
     * Mark `key` as deleted.  After commit the key will read as `undefined`.
     */
    clear(key: Kin): void {
        const serialised = this.serialiseKey(key);
        this._writeBuffer.set(serialised, TOMBSTONE);
    }

    /**
     * Return a scoped transaction that shares this transaction's read-operations,
     * write-buffer, and versionMap but uses a different subspace for key/value
     * encoding.  This mirrors FoundationDB's `txn.at(subspace)` — all reads
     * and writes flow through the same transaction and are committed atomically.
     *
     * Throws if the subspace belongs to a different store (different versionMap).
     */
    at<SubKeyIn, SubKeyOut, SubValIn, SubValOut>(
        subspace: ISubspace<SubKeyIn, SubKeyOut, SubValIn, SubValOut>,
    ): ITransaction<SubKeyIn, SubKeyOut, SubValIn, SubValOut> {
        return new Transaction<SubKeyIn, SubKeyOut, SubValIn, SubValOut>(
            this.readVersion,
            subspace,
            this.ryow,
            this._readOperations,
            this._writeBuffer,
            this.versionMap,
        );
    }

    /**
     * Return a snapshot view of this transaction.  Reads through the returned
     * transaction resolve values at the same snapshot version and see the same
     * write buffer, but they are **not** recorded as read operations — so they
     * will never cause conflicts.  Writes still go into the shared write buffer
     * and are committed normally.
     *
     * This mirrors FoundationDB's `txn.snapshot` property.
     */
    snapshot(): ITransaction<Kin, KOut, Vin, VOut> {
        // Pass a throwaway readOps array so reads aren't tracked.
        return new Transaction<Kin, KOut, Vin, VOut>(
            this.readVersion,
            this.subspace,
            this.ryow,
            [],               // discarded — reads won't cause conflicts
            this._writeBuffer, // shared — writes are committed
            this.versionMap,   // shared — reads resolve the same data
        );
    }

    // ---------------------------------------------------------------------------
    // Internal — exposed for the commit phase in Store
    // ---------------------------------------------------------------------------

    /** @internal */
    get readOperations(): ReadonlyArray<ReadOperation> {
        return this._readOperations;
    }

    /** @internal */
    get writeBuffer() {
        return this._writeBuffer;
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    /**
     * Merge two sorted `[serialisedKey, KOut, VOut][]` arrays into a single
     * `[KOut, VOut][]` preserving sorted serialised-key order.
     */
    private mergeSorted(
        a: [string, KOut, VOut][],
        b: [string, KOut, VOut][],
    ): [KOut, VOut][] {
        b.sort((x, y) => x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0);
        const out: [KOut, VOut][] = [];
        let ai = 0;
        let bi = 0;
        while (ai < a.length && bi < b.length) {
            if (a[ai]![0] <= b[bi]![0]) {
                out.push([a[ai]![1], a[ai]![2]]);
                ai++;
            } else {
                out.push([b[bi]![1], b[bi]![2]]);
                bi++;
            }
        }
        while (ai < a.length) { out.push([a[ai]![1], a[ai]![2]]); ai++; }
        while (bi < b.length) { out.push([b[bi]![1], b[bi]![2]]); bi++; }
        return out;
    }

    private serialiseKey(key: Kin): string {
        return this.subspace.packKey(key).toString("hex");
    }
    private deserializeKey(serialisedKey: string): KOut {
        return this.subspace.unpackKey(Buffer.from(serialisedKey, "hex"));
    }

    /**
     * Resolve the visible value for a serialised key, checking the local write
     * buffer first, then falling back to the versioned store.
     * Returns `undefined` if the key is absent or tombstoned.
     */
    private resolveValue(serialisedKey: string): VOut | undefined {
        if (this.ryow && this._writeBuffer.has(serialisedKey)) {
            const buffered = this._writeBuffer.get(serialisedKey);
            return (buffered === undefined || buffered === TOMBSTONE)
                ? undefined
                : (this.subspace.unpackValue(Buffer.from(buffered)));
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
    private readFromStore(serialisedKey: string): VOut | undefined {
        const entries = this.versionMap.get(serialisedKey);
        if (!entries || entries.length === 0) {
            return undefined;
        }

        // Walk backwards to find the newest entry visible at readVersion.
        for (let i = entries.length - 1; i >= 0; i--) {
            const entry = entries[i]!;
            if (entry.version <= this.readVersion) {
                return entry.value === TOMBSTONE ? undefined : this.subspace.unpackValue(Buffer.from(entry.value as Buffer));
            }
        }

        return undefined;
    }
}
