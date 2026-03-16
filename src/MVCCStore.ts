import stringify from "json-stable-stringify";
import { Transaction } from "./Transaction";
import {
  TOMBSTONE,
  ConflictError,
  type ReadOperation,
  type TransactionOptions,
  type VersionedEntry,
} from "./types";

const DEFAULT_MAX_RETRIES = 5;

/**
 * An in-memory, MVCC-based transactional key/value store inspired by
 * FoundationDB's node bindings.
 *
 * Data is versioned — every committed write creates a new version entry so
 * that concurrent readers continue to see a consistent snapshot.  Conflict
 * detection uses optimistic concurrency control: at commit time the store
 * walks the transaction's recorded read-operations (a discriminated union of
 * point reads and filter reads) and checks for conflicts.  On conflict the
 * transaction callback is automatically re-executed (up to `maxRetries`).
 *
 * Usage mirrors the FoundationDB pattern:
 *
 * ```ts
 * const store = new MVCCStore<MyKey, MyVal>();
 *
 * const result = await store.doTransaction(async (txn) => {
 *   const val = txn.get({ id: 1 });
 *   txn.set({ id: 1 }, { ...val, count: (val?.count ?? 0) + 1 });
 *   return val;
 * });
 * ```
 *
 * @typeParam K - Key type (must be JSON-serialisable).
 * @typeParam V - Value type.
 */
export class MVCCStore<K, V> {
  /**
   * Monotonically increasing version counter.
   * Incremented on every successful commit.
   */
  private currentVersion = 0;

  /**
   * The core data structure.
   * Maps a *serialised* key string → ordered list of versioned entries.
   * Entries are appended in commit-version order (ascending).
   */
  private readonly versionMap = new Map<string, VersionedEntry<V>[]>();

  /**
   * Tracks read versions held by in-flight transactions.
   * Maps `readVersion → reference count` (multiple concurrent transactions
   * can share the same snapshot version).
   */
  private readonly activeReadVersions = new Map<number, number>();

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Execute `callback` inside a transaction with automatic retry on conflict.
   *
   * The callback receives a `Transaction` instance exposing `get`, `set`,
   * `clear`, and `getUsingFilter`.  When the callback's returned promise
   * resolves, the store will attempt to commit.  If a conflict is detected
   * the callback will be re-invoked from scratch (reads + writes reset) up
   * to `maxRetries` times.
   *
   * Returns the value returned by the callback on a successful commit.
   */
  async doTransaction<R>(
    callback: (txn: Transaction<K, V>) => Promise<R>,
    options?: TransactionOptions,
  ): Promise<R> {
    const maxRetries = options?.maxRetries ?? DEFAULT_MAX_RETRIES;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      // 1. Snapshot the current version.
      const readVersion = this.acquireReadVersion();

      try {
        // 2. Create a fresh transaction bound to this snapshot.
        const txn = new Transaction<K, V>(
          readVersion,
          this.versionMap,
          this.serialize,
          this.deserialize,
          options?.readYourOwnWrites ?? true,
        );

        // 3. Run the user's callback.
        const result = await callback(txn);

        // 4. Commit phase — synchronous, so it cannot be interleaved by other
        //    microtasks.  This gives us atomicity in a single-threaded runtime.
        const conflict = this.detectConflict(txn, readVersion);

        if (conflict) {
          // Retry: loop back to create a new snapshot and transaction.
          continue;
        }

        // No conflict — apply the write buffer and trim old versions.
        this.applyWrites(txn);
        this.trimVersions();

        return result;
      } finally {
        this.releaseReadVersion(readVersion);
      }
    }

    // All retries exhausted.
    throw new ConflictError();
  }

  /**
   * Return the current commit version (useful for diagnostics / testing).
   */
  get version(): number {
    return this.currentVersion;
  }

  // ---------------------------------------------------------------------------
  // Read-version lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Register a new read-version hold.  Returns the snapshot version.
   */
  private acquireReadVersion(): number {
    const v = this.currentVersion;
    this.activeReadVersions.set(
      v,
      (this.activeReadVersions.get(v) ?? 0) + 1,
    );
    return v;
  }

  /**
   * Release a previously-acquired read-version hold.
   */
  private releaseReadVersion(v: number): void {
    const count = this.activeReadVersions.get(v) ?? 0;
    if (count <= 1) {
      this.activeReadVersions.delete(v);
    } else {
      this.activeReadVersions.set(v, count - 1);
    }
  }

  // ---------------------------------------------------------------------------
  // Version trimming (GC)
  // ---------------------------------------------------------------------------

  /**
   * Remove version entries that can no longer be read by any active
   * transaction.  For each key we keep the **latest** entry whose version
   * is ≤ the minimum active read-version (that entry is still needed as the
   * snapshot-visible value), plus every entry newer than that.  Everything
   * older is discarded.
   *
   * Called automatically after every successful commit.
   */
  private trimVersions(): void {
    // If there are active readers, the safe trim horizon is the oldest
    // snapshot still in use.  Otherwise we can collapse to just the latest.
    const minActive =
      this.activeReadVersions.size > 0
        ? Math.min(...this.activeReadVersions.keys())
        : this.currentVersion;

    for (const [serialisedKey, entries] of this.versionMap) {
      if (entries.length <= 1) {
        continue; // nothing to trim
      }

      // Find the index of the latest entry with version ≤ minActive.
      // Everything before that index is unreachable.
      let pivotIndex = -1;
      for (let i = entries.length - 1; i >= 0; i--) {
        if (entries[i]!.version <= minActive) {
          pivotIndex = i;
          break;
        }
      }

      if (pivotIndex > 0) {
        // Remove entries [0 .. pivotIndex-1], keep [pivotIndex .. end].
        entries.splice(0, pivotIndex);
      }

      // Clean up fully-empty keys (all entries trimmed is unlikely here,
      // but guard defensively).
      if (entries.length === 0) {
        this.versionMap.delete(serialisedKey);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Deterministically serialise a key object using `json-stable-stringify`
   * so that structurally identical objects always map to the same string.
   *
   * Bound as an arrow function so it can be safely passed by reference to
   * `Transaction`.
   */
  private serialize = (key: K): string => {
    const result = stringify(key);
    if (result === undefined) {
      throw new TypeError(
        "Key cannot be serialised to JSON: " + String(key),
      );
    }
    return result;
  };

  /**
   * Deserialise a serialised key string back to the original key object.
   * Bound as an arrow function so it can be passed by reference.
   */
  private deserialize = (serialised: string): K => {
    return JSON.parse(serialised) as K;
  };

  // ---------------------------------------------------------------------------
  // Conflict detection
  // ---------------------------------------------------------------------------

  /**
   * Walk every recorded read operation in the transaction and check for
   * conflicts.  Returns `true` if any operation conflicts.
   */
  private detectConflict(
    txn: Transaction<K, V>,
    readVersion: number,
  ): boolean {
    for (const op of txn.readOperations) {
      if (this.processReadOperationForConflicts(op, readVersion)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Dispatch a single `ReadOperation` and return `true` if it conflicts.
   *
   * - **read**: the key was written at a version newer than `readVersion`.
   * - **filterRead**: the set of keys matching the filter has changed
   *   (additions or removals).  Value changes on individual matched keys
   *   are already covered by the companion `read` operations that
   *   `getUsingFilter` records for each matched row.
   */
  private processReadOperationForConflicts(
    op: ReadOperation<K, V>,
    readVersion: number,
  ): boolean {
    switch (op.type) {
      case "read":
        return this.checkKeyConflict(op.key, readVersion);

      case "filterRead":
        return this.checkFilterConflict(op, readVersion);
    }
  }

  /**
   * Returns `true` if the given serialised key has been written at any
   * version after `readVersion`.
   */
  private checkKeyConflict(
    serialisedKey: string,
    readVersion: number,
  ): boolean {
    const entries = this.versionMap.get(serialisedKey);
    if (!entries || entries.length === 0) {
      return false;
    }
    // Entries are appended in ascending version order — check the last one.
    const latest = entries[entries.length - 1]!;
    return latest.version > readVersion;
  }

  /**
   * Re-run the filter against the *current* store state and compare the
   * resulting set of matched keys with the set captured at snapshot time.
   * Returns `true` if any keys were added or removed (i.e. the membership
   * of the result set changed).
   *
   * Value changes on previously-matched rows do NOT need checking here —
   * those are caught by the individual `read` operations recorded alongside
   * the `filterRead`.
   */
  private checkFilterConflict(
    op: { filter: (key: K, value: V) => boolean; matchedKeys: Set<string> },
    _readVersion: number,
  ): boolean {
    const currentMatchedKeys = new Set<string>();

    for (const [serialisedKey, entries] of this.versionMap) {
      if (!entries || entries.length === 0) {
        continue;
      }

      // Resolve the *latest* committed value (current state, not snapshot).
      const latest = entries[entries.length - 1]!;
      if (latest.value === TOMBSTONE) {
        // Key is currently deleted.  If it was in the original matched set,
        // that's a removal → conflict (but that would already be caught by
        // the companion `read` op, so we still track it here for
        // completeness of set-diff detection).
        if (op.matchedKeys.has(serialisedKey)) {
          return true;
        }
        continue;
      }

      const key = this.deserialize(serialisedKey);
      if (op.filter(key, latest.value)) {
        currentMatchedKeys.add(serialisedKey);
      }
    }

    // Detect additions: keys now matching that didn't match at snapshot time.
    for (const k of currentMatchedKeys) {
      if (!op.matchedKeys.has(k)) {
        return true;
      }
    }

    // Detect removals: keys that matched at snapshot time but no longer match.
    for (const k of op.matchedKeys) {
      if (!currentMatchedKeys.has(k)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Apply the transaction's buffered writes to the version map and bump
   * `currentVersion`.  Must be called only when no conflict was detected.
   */
  private applyWrites(txn: Transaction<K, V>): void {
    if (txn.writeBuffer.size === 0) {
      return; // read-only transaction — nothing to commit.
    }

    this.currentVersion++;
    const commitVersion = this.currentVersion;

    for (const [serialisedKey, value] of txn.writeBuffer) {
      let entries = this.versionMap.get(serialisedKey);
      if (!entries) {
        entries = [];
        this.versionMap.set(serialisedKey, entries);
      }
      entries.push({
        version: commitVersion,
        value: value as V | typeof TOMBSTONE,
      });
    }
  }
}
