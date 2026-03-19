import { OrderedMap } from "./OrderedMap";

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
export interface VersionedEntry {
    version: number;
    value: string | Buffer | Tombstone;
}

/**
 * Options accepted by `Store.doTransaction`.
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
 * A scan-based read (filter scan, range scan, etc.).  At commit time the
 * store calls `recheck` to obtain the current set of matching serialised
 * keys and compares it to `matchedKeys` captured at snapshot time.  If
 * membership changed (additions or removals) the transaction conflicts.
 *
 * Individual matched rows are *also* recorded as `KeyReadOperation` entries
 * so value-change conflicts are caught automatically.
 */
export interface ScanReadOperation {
    type: "scanRead";
    /**
     * Re-execute the scan against the *current* committed state of the
     * version map and return the set of serialised keys that match.
     */
    recheck: (versionMap: OrderedMap<string, VersionedEntry[]>) => Set<string>;
    /** Serialised keys that matched at snapshot time. */
    matchedKeys: Set<string>;
}

/**
 * Discriminated union of all read-operation kinds tracked by a transaction.
 */
export type ReadOperation = KeyReadOperation | ScanReadOperation;

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


export interface Transformer<VIN, VOUT> {
    pack: (value: VIN) => Buffer;
    unpack: (buffer: Buffer) => VOUT;
}

// ---------------------------------------------------------------------------
// ISubspace — invariant subspace interface
// ---------------------------------------------------------------------------

/**
 * An invariant interface for subspaces.
 *
 * Uses arrow-function property syntax (like {@link ITransaction}) so that
 * the type parameters are invariant — you cannot pass off an
 * `ISubspace<X & Y, …>` as an `ISubspace<X, …>` unless the types are
 * compatible in both directions.
 */
export interface ISubspace<KeyIn, KeyOut, ValIn, ValOut> {
    readonly prefix?: string;
    readonly versionMap?: OrderedMap<string, VersionedEntry[]>;
    contains: (serialisedKeyHex: string) => boolean;
    packKey: (key: KeyIn) => string | Buffer;
    unpackKey: (key: Buffer) => KeyOut;
    packValue: (val: ValIn) => string | Buffer;
    unpackValue: (val: Buffer) => ValOut;
    withKeyEncoding: <NewKeyIn, NewKeyOut>(keyXf: Transformer<NewKeyIn, NewKeyOut>) => ISubspace<NewKeyIn, NewKeyOut, ValIn, ValOut>;
}

export interface RangeOptions {
    limit?: number;
    reverse?: boolean;
}

// ---------------------------------------------------------------------------
// ITransaction — FDB-compatible interface
// ---------------------------------------------------------------------------

/**
 * A FoundationDB-compatible transaction interface.
 *
 * In FoundationDB the read methods (`get`, `getRangeAll`,
 * `getRangeAllStartsWith`) return `Promise<T>`.  In this in-memory MVCC
 * store they return `T` synchronously.  This interface accommodates both by
 * typing return values as `T | Promise<T>`, so code written against
 * `ITransaction` works with either implementation.
 * Note that we use arrow functions here to ensure that the methods are invariantly bound to the transaction instance, which is important so
 * that i cannot pass off an iTransaction<X&Y> as an iTransaction<X> or iTransaction<Y> unless X & Y are compatible.
 */
export interface ITransaction<Kin, KOut, Vin, VOut> {
    get: (key: Kin) => VOut | undefined | Promise<VOut | undefined>;
    set: (key: Kin, value: Vin) => void;
    clear: (key: Kin) => void;
    getRangeAll: (
        start: Kin,
        end: Kin,
        opts?: RangeOptions,
    ) => [KOut, VOut][] | Promise<[KOut, VOut][]>;
    getRangeAllStartsWith: (
        prefix: Kin,
        opts?: RangeOptions,
    ) => [KOut, VOut][] | Promise<[KOut, VOut][]>;
    getRange: (
        start: Kin,
        end: Kin,
        opts?: RangeOptions,
    ) => AsyncGenerator<[KOut, VOut]>;
    at: <SubKeyIn, SubKeyOut, SubValIn, SubValOut>(
        subspace: ISubspace<SubKeyIn, SubKeyOut, SubValIn, SubValOut>,
    ) => ITransaction<SubKeyIn, SubKeyOut, SubValIn, SubValOut>;
    snapshot: () => ITransaction<Kin, KOut, Vin, VOut>;
}


export type TransactionFactory<K, V> = <R>(callback: (txn: ITransaction<K, K, V, V>) => Promise<R>, options?: TransactionOptions) => Promise<R>;
