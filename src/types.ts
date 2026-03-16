/**
 * Unique sentinel value used to represent a cleared (deleted) key in the
 * MVCC version history.  Storing a tombstone rather than removing the entry
 * lets us distinguish "key was explicitly deleted" from "key was never set".
 */
export const TOMBSTONE: unique symbol = Symbol("TOMBSTONE");
export type Tombstone = typeof TOMBSTONE;

/**
 * A single versioned entry stored for a key.
 * `version` is the commit-version at which this value was written.
 * `value` is either the real value or `TOMBSTONE` if the key was cleared.
 */
export interface VersionedEntry<V> {
  version: number;
  value: V | Tombstone;
}

/**
 * Options accepted by `MVCCStore.doTransaction`.
 */
export interface TransactionOptions {
  /**
   * Maximum number of times the transaction callback will be re-executed
   * after a conflict before giving up and throwing a `ConflictError`.
   * Defaults to 5.
   */
  maxRetries?: number;

  /**
   * When `true` (the default), reads within a transaction see uncommitted
   * writes made earlier in the same transaction.  Set to `false` to always
   * read from the snapshot, ignoring the local write buffer.
   */
  readYourOwnWrites?: boolean;
}

// ---------------------------------------------------------------------------
// Read operations — discriminated union
// ---------------------------------------------------------------------------

/**
 * A point read of a single key.  Conflict is detected if the key was written
 * at any version after the transaction's snapshot.
 */
export interface KeyReadOperation {
  type: "read";
  /** Serialised key string. */
  key: string;
}

/**
 * A filter-based scan.  Stores the filter callback and the set of serialised
 * keys that matched at read time.  Individual matched rows are *also*
 * recorded as `KeyReadOperation` entries (so value-change conflicts on
 * matched keys are caught automatically).  At commit time the filter is
 * re-evaluated against the current store to detect keys that were added to
 * or removed from the result set.
 */
export interface FilterReadOperation<K, V> {
  type: "filterRead";
  /** The filter callback supplied by the caller. */
  filter: (key: K, value: V) => boolean;
  /** Serialised keys that matched the filter at snapshot time. */
  matchedKeys: Set<string>;
}

/**
 * Discriminated union of all read-operation kinds tracked by a transaction.
 */
export type ReadOperation<K, V> = KeyReadOperation | FilterReadOperation<K, V>;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/**
 * Thrown when a transaction cannot be committed because a key it read was
 * modified by another transaction after its snapshot version, and all
 * automatic retries have been exhausted.
 */
export class ConflictError extends Error {
  constructor(message?: string) {
    super(message ?? "Transaction conflict: maximum retries exceeded");
    this.name = "ConflictError";
  }
}
