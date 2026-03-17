/**
 * A generic ordered map that keeps entries sorted by key while providing
 * O(1) key→value lookups via an internal hash map and O(log n) insertions
 * via binary search into a sorted array.
 *
 * The sorted array is exposed directly as `entries` for efficient ordered
 * iteration / slicing.
 *
 * @typeParam K - Key type (must be usable as a Map key).
 * @typeParam V - Value type.
 */
export class OrderedMap<K extends string, V> {
    /** Fast key→index+value lookup. */
    private readonly map = new Map<K, V>();

    /** Entries maintained in sorted key order. */
    private readonly sorted: [K, V][] = [];

    /**
     * @param compare  A comparator for keys.  Must return a negative number
     *                 if `a < b`, 0 if equal, positive if `a > b`.
     */
    constructor(private readonly compare: (a: K, b: K) => number) { }

    // -----------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------

    /** O(1) key lookup. */
    get(key: K): V | undefined {
        return this.map.get(key);
    }

    /** O(1) existence check. */
    has(key: K): boolean {
        return this.map.has(key);
    }

    /** Number of entries in the map. */
    get size(): number {
        return this.map.size;
    }

    /**
     * The full sorted entry list.  This is the *live* backing array — treat
     * it as read-only.  Use `entriesSlice` or spread if you need a copy.
     */
    get entries(): ReadonlyArray<readonly [K, V]> {
        return this.sorted;
    }

    /** All keys in sorted order. */
    keys(): K[] {
        return this.sorted.map(([k]) => k);
    }

    /** All values in key-sorted order. */
    values(): V[] {
        return this.sorted.map(([, v]) => v);
    }

    // -----------------------------------------------------------------------
    // Writes
    // -----------------------------------------------------------------------

    /**
     * Insert or update `key` with `value`, maintaining sorted order.
     *
     * - New key:    O(log n) search + O(n) splice.
     * - Existing:   O(log n) search + O(1) in-place update.
     */
    set(key: K, value: V): this {
        if (this.map.has(key)) {
            // Update existing — find its position and patch in-place.
            const idx = this.indexOf(key);
            this.sorted[idx] = [key, value];
            this.map.set(key, value);
            return this;
        }

        // New key — binary search for the insertion point.
        const idx = this.insertionIndex(key);
        this.sorted.splice(idx, 0, [key, value]);
        this.map.set(key, value);
        return this;
    }

    /** Remove `key`.  Returns `true` if the key existed. */
    delete(key: K): boolean {
        if (!this.map.has(key)) return false;

        const idx = this.indexOf(key);
        this.sorted.splice(idx, 1);
        this.map.delete(key);
        return true;
    }

    /** Remove all entries. */
    clear(): void {
        this.sorted.length = 0;
        this.map.clear();
    }

    // -----------------------------------------------------------------------
    // Iteration helpers
    // -----------------------------------------------------------------------

    /** Iterate entries in sorted order. */
    [Symbol.iterator](): IterableIterator<[K, V]> {
        return this.sorted[Symbol.iterator]();
    }

    /** Invoke `fn` for each entry in sorted order. */
    forEach(fn: (value: V, key: K) => void): void {
        for (const [k, v] of this.sorted) {
            fn(v, k);
        }
    }

    /**
     * Iterate entries where `key >= begin` and `key < end`.
     *
     * Both bounds use O(log n) binary search to find the slice boundaries,
     * then yield entries from the sorted array.
     *
     * @param begin   Inclusive lower bound.
     * @param end     Exclusive upper bound.
     * @param opts.reverse  When `true`, entries are yielded from high→low.
     *                      Defaults to `false` (low→high).
     */
    getRange(
        begin: K,
        end: K,
        opts?: { reverse?: boolean },
    ): [K, V][] {
        // Find first index where key >= begin.
        const lo = this.insertionIndex(begin);
        // Find first index where key >= end (everything before it is < end).
        const hi = this.insertionIndex(end);

        if (lo >= hi) return [];

        const slice = this.sorted.slice(lo, hi);
        if (opts?.reverse) slice.reverse();
        return slice.map(([k, v]) => [k, v]);
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /**
     * Binary search for the index where `key` should be inserted to maintain
     * sorted order.  Returns the first index whose key is ≥ `key`.
     */
    private insertionIndex(key: K): number {
        let lo = 0;
        let hi = this.sorted.length;
        while (lo < hi) {
            const mid = (lo + hi) >>> 1;
            if (this.compare(this.sorted[mid]![0], key) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * Binary search for the exact index of `key`.
     * Assumes the key exists (caller must check `map.has` first).
     */
    private indexOf(key: K): number {
        let lo = 0;
        let hi = this.sorted.length;
        while (lo < hi) {
            const mid = (lo + hi) >>> 1;
            const cmp = this.compare(this.sorted[mid]![0], key);
            if (cmp === 0) return mid;
            if (cmp < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo; // should always hit the === 0 branch above
    }
}
