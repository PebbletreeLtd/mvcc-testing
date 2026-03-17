import { Store } from "./Store";
import Subspace from "./subspace";
import {
    TOMBSTONE,
    type ITransaction,
    type Tombstone,
    type TransactionOptions,
    type Transformer,
} from "./types";

/** Singleton empty buffer written as the value for every derived entry. */
const EMPTY_BUF = Buffer.alloc(0);

/**
 * A read-only, automatically-maintained secondary index (derived view) over
 * an {@link MVCCStore}.
 *
 * Unlike the previous implementation this is a **Subspace**, not a store.
 * Derived entries are written directly into the **source** store's
 * `versionMap` (namespaced by this subspace's prefix), so
 * `txn.at(derivedStore)` inside a source transaction sees the index
 * entries without any cross-store plumbing.
 *
 * The value-input type parameter is fixed to `never` so that `set()` is
 * uncallable at compile time.  A runtime guard in {@link doTransaction}
 * also prevents any writes that sneak past the type system (e.g. `clear`).
 *
 * @typeParam Kin   - Source store key-in type.
 * @typeParam KOut  - Source store key-out type.
 * @typeParam Vin   - Source store value-in type.
 * @typeParam VOut  - Source store value-out type.
 * @typeParam FKIn  - Derived (index) key-in type.
 * @typeParam FKOut - Derived (index) key-out type (must extend FKIn).
 */
export class DerivedMVCCStore<
    Kin,
    KOut,
    Vin,
    VOut,
    FKIn,
    FKOut extends FKIn,
> extends Subspace<FKIn, FKOut, never, unknown> {
    private readonly _source: Store<Kin, KOut, Vin, VOut>;
    private readonly _mapKey: (key: KOut, value: VOut) => FKIn;

    constructor(args: {
        source: Store<Kin, KOut, Vin, VOut>;
        /** Project a source key/value pair into the derived key. */
        mapKey: (key: KOut, value: VOut) => FKIn;
        /** Transformer for the derived key type. */
        keyTransformer: Transformer<FKIn, FKOut>;
        /** Optional prefix for the derived key space. */
        prefix?: string;
    }) {
        super(args.keyTransformer, args.prefix);

        this._source = args.source;
        this._mapKey = args.mapKey;

        // Back-fill from the source store's current state.
        this._backfill();

        // Register a post-commit hook so future source writes are
        // projected into the source's versionMap under our prefix.
        this._source.onCommit((writes, commitVersion) => {
            this._handleSourceCommit(writes, commitVersion);
        });
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /** Values are always empty buffers — return an empty object. */
    override unpackValue(_val: Buffer): unknown {
        return {};
    }

    /** Current commit version — delegates to the source store. */
    get version(): number {
        return this._source.version;
    }

    /**
     * Execute a read-only transaction against the derived index.
     *
     * Internally delegates to the source store's `doTransaction` and
     * scopes the callback through `txn.at(this)`, so reads resolve from
     * the source's `versionMap` where derived entries are stored.
     *
     * Any writes attempted through the callback will throw.
     */
    async doTransaction<R>(
        callback: (txn: ITransaction<FKIn, FKOut, never, unknown>) => Promise<R>,
        options?: TransactionOptions,
    ): Promise<R> {
        return this._source.doTransaction(async (txn) => {
            const scopedTxn = txn.at(this);
            const sizeBefore = txn.writeBuffer.size;
            const result = await callback(scopedTxn);
            if (txn.writeBuffer.size > sizeBefore) {
                throw new Error("DerivedMVCCStore is read-only");
            }
            return result;
        }, options);
    }

    /** Alias for {@link doTransaction}. */
    doTn<R>(
        callback: (txn: ITransaction<FKIn, FKOut, never, unknown>) => Promise<R>,
        options?: TransactionOptions,
    ): Promise<R> {
        return this.doTransaction(callback, options);
    }

    // -----------------------------------------------------------------------
    // Back-fill
    // -----------------------------------------------------------------------

    /**
     * Populate the source's versionMap with derived entries from the
     * source store's current state.  Called once during construction.
     */
    private _backfill(): void {
        for (const [serialisedKey, entries] of this._source.versionMap) {
            if (entries.length === 0) continue;
            const latest = entries[entries.length - 1]!;
            if (latest.value === TOMBSTONE) continue;

            // Skip keys that don't belong to the source subspace.
            if (!this._source.contains(serialisedKey)) continue;

            const srcKey = this._decodeSourceKey(serialisedKey);
            const srcValue = this._decodeSourceValue(latest.value);

            const derivedKey = this._mapKey(srcKey, srcValue);
            const derivedKeyHex = this._derivedKeyHex(derivedKey);

            this._writeEntry(derivedKeyHex, EMPTY_BUF, latest.version);
        }
    }

    // -----------------------------------------------------------------------
    // Post-commit hook handler
    // -----------------------------------------------------------------------

    /**
     * Called synchronously by the source store after each commit.
     * Projects the source writes into the source's `versionMap` under
     * this subspace's prefix.
     */
    private _handleSourceCommit(
        writes: ReadonlyMap<string, string | Buffer | Tombstone>,
        commitVersion: number,
    ): void {
        for (const [serialisedKey, value] of writes) {
            // Skip keys that don't belong to the source subspace.
            if (!this._source.contains(serialisedKey)) continue;

            const srcKey = this._decodeSourceKey(serialisedKey);

            if (value === TOMBSTONE) {
                this._handleTombstone(serialisedKey, srcKey, commitVersion);
            } else {
                this._handleSet(serialisedKey, srcKey, value, commitVersion);
            }
        }
    }

    /**
     * A source key was cleared — find the previous value so we can derive
     * the old index key and tombstone it.
     */
    private _handleTombstone(
        serialisedKey: string,
        srcKey: KOut,
        commitVersion: number,
    ): void {
        const prevValue = this._previousSourceValue(serialisedKey, commitVersion);
        if (prevValue === undefined) return; // nothing to undo

        const oldDerivedKeyHex = this._derivedKeyHex(
            this._mapKey(srcKey, prevValue),
        );
        this._writeEntry(oldDerivedKeyHex, TOMBSTONE, commitVersion);
    }

    /**
     * A source key was set — compute the new derived entry and write it.
     * If the source key previously existed with a *different* derived key,
     * tombstone the old one first.
     */
    private _handleSet(
        serialisedKey: string,
        srcKey: KOut,
        rawValue: string | Buffer,
        commitVersion: number,
    ): void {
        const srcValue = this._decodeSourceValue(rawValue);
        const derivedKey = this._mapKey(srcKey, srcValue);
        const derivedKeyHex = this._derivedKeyHex(derivedKey);

        // Check whether the derived key changed (value update that moves
        // the index entry to a different key).
        const prevSrcValue = this._previousSourceValue(serialisedKey, commitVersion);
        if (prevSrcValue !== undefined) {
            const oldDerivedKeyHex = this._derivedKeyHex(
                this._mapKey(srcKey, prevSrcValue),
            );
            if (oldDerivedKeyHex !== derivedKeyHex) {
                this._writeEntry(oldDerivedKeyHex, TOMBSTONE, commitVersion);
            }
        }

        this._writeEntry(derivedKeyHex, EMPTY_BUF, commitVersion);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Decode a source key from its hex-encoded packed representation. */
    private _decodeSourceKey(hex: string): KOut {
        return this._source.unpackKey(Buffer.from(hex, "hex"));
    }

    /** Decode a source value from its packed representation. */
    private _decodeSourceValue(raw: string | Buffer): VOut {
        return JSON.parse(typeof raw === "string" ? raw : raw.toString()) as VOut;
    }

    /** Pack a derived key to its hex string for use as a versionMap key. */
    private _derivedKeyHex(key: FKIn): string {
        return (this.packKey(key) as Buffer).toString("hex");
    }

    /**
     * Look up the source value for `serialisedKey` at the version immediately
     * preceding `commitVersion`.  Returns `undefined` if there was no prior
     * live value.
     */
    private _previousSourceValue(
        serialisedKey: string,
        commitVersion: number,
    ): VOut | undefined {
        const entries = this._source.versionMap.get(serialisedKey);
        if (!entries) return undefined;

        for (let i = entries.length - 1; i >= 0; i--) {
            const e = entries[i]!;
            if (e.version < commitVersion) {
                if (e.value === TOMBSTONE) return undefined;
                return this._decodeSourceValue(e.value);
            }
        }
        return undefined;
    }

    /**
     * Low-level helper — write a single entry into the source store's
     * versionMap (under this subspace's prefix).
     */
    private _writeEntry(
        derivedKeyHex: string,
        value: string | Buffer | Tombstone,
        version: number,
    ): void {
        let entries = this._source.versionMap.get(derivedKeyHex);
        if (!entries) {
            entries = [];
            this._source.versionMap.set(derivedKeyHex, entries);
        }
        entries.push({ version, value });
    }
}
