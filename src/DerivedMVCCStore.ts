import { MVCCStore } from "./MVCCStore";
import { Transaction } from "./Transaction";
import {
    TOMBSTONE,
    type Tombstone,
    type TransactionOptions,
    type Transformer,
} from "./types";

/**
 * A read-only, automatically-maintained secondary index (derived view) over
 * an {@link MVCCStore}.
 *
 * On every commit to the source store the `mapKey` / `mapValue` projections
 * are applied and the results written into this store's own `versionMap`.
 * Reads go through the normal MVCC transaction path, so consumers get
 * snapshot isolation, conflict detection, etc. for free.
 *
 * The value-input type parameter is fixed to `never` so that `set()` is
 * uncallable at compile time.  A runtime guard in {@link doTransaction} also
 * prevents any writes that sneak past the type system (e.g. `clear`).
 *
 * @typeParam Kin  - Source store key-in type.
 * @typeParam KOut - Source store key-out type.
 * @typeParam Vin  - Source store value-in type.
 * @typeParam VOut - Source store value-out type.
 * @typeParam FK   - Derived (index) key type.
 * @typeParam FVOut - Derived (index) value type.
 */
export class DerivedMVCCStore<
    Kin,
    KOut,
    Vin,
    VOut,
    FK,
    FVOut,
> extends MVCCStore<FK, FK, never, FVOut> {
    private readonly _source: MVCCStore<Kin, KOut, Vin, VOut>;
    private readonly _mapKey: (key: KOut, value: VOut) => FK;
    private readonly _mapValue: (key: KOut, value: VOut) => FVOut;

    constructor(args: {
        source: MVCCStore<Kin, KOut, Vin, VOut>;
        /** Project a source key/value pair into the derived key. */
        mapKey: (key: KOut, value: VOut) => FK;
        /** Project a source key/value pair into the derived value. */
        mapValue: (key: KOut, value: VOut) => FVOut;
        /** Transformer for the derived key type. */
        keyTransformer: Transformer<FK, FK>;
    }) {
        super({ keyTransformer: args.keyTransformer });

        this._source = args.source;
        this._mapKey = args.mapKey;
        this._mapValue = args.mapValue;

        // Back-fill from the source store's current state.
        this._backfill();

        // Register a post-commit hook so future source writes are
        // projected into this derived store automatically.
        this._source.onCommit((writes, commitVersion) => {
            this._handleSourceCommit(writes, commitVersion);
        });
    }

    // -----------------------------------------------------------------------
    // Read-only enforcement
    // -----------------------------------------------------------------------

    /**
     * Overridden to enforce read-only semantics.  The `never` value-input
     * type makes `set()` uncallable, but `clear()` can still be invoked at
     * runtime.  This guard catches any such attempt.
     */
    override async doTransaction<R>(
        callback: (txn: Transaction<FK, FK, never, FVOut>) => Promise<R>,
        options?: TransactionOptions,
    ): Promise<R> {
        return super.doTransaction(async (txn) => {
            const result = await callback(txn);
            if (txn.writeBuffer.size > 0) {
                throw new Error("DerivedMVCCStore is read-only");
            }
            return result;
        }, options);
    }

    // -----------------------------------------------------------------------
    // Back-fill
    // -----------------------------------------------------------------------

    /**
     * Populate the derived versionMap from the source store's current state.
     * Called once during construction.
     */
    private _backfill(): void {
        for (const [serialisedKey, entries] of this._source.versionMap) {
            if (entries.length === 0) continue;
            const latest = entries[entries.length - 1]!;
            if (latest.value === TOMBSTONE) continue;

            const srcKey = this._decodeSourceKey(serialisedKey);
            const srcValue = this._decodeSourceValue(latest.value);

            const derivedKey = this._mapKey(srcKey, srcValue);
            const derivedValue = this._mapValue(srcKey, srcValue);
            const derivedKeyHex = this._derivedKeyHex(derivedKey);
            const derivedValuePacked = JSON.stringify(derivedValue);

            let dEntries = this.versionMap.get(derivedKeyHex);
            if (!dEntries) {
                dEntries = [];
                this.versionMap.set(derivedKeyHex, dEntries);
            }
            dEntries.push({ version: latest.version, value: derivedValuePacked });
        }

        // Align our version counter with the source.
        this.currentVersion = this._source.version;
    }

    // -----------------------------------------------------------------------
    // Post-commit hook handler
    // -----------------------------------------------------------------------

    /**
     * Called synchronously by the source store after each commit.
     * Projects the source writes into this derived store's `versionMap`.
     */
    private _handleSourceCommit(
        writes: ReadonlyMap<string, string | Buffer | Tombstone>,
        commitVersion: number,
    ): void {
        this.currentVersion = commitVersion;

        for (const [serialisedKey, value] of writes) {
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
        const derivedValue = this._mapValue(srcKey, srcValue);
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

        this._writeEntry(derivedKeyHex, JSON.stringify(derivedValue), commitVersion);
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
    private _derivedKeyHex(key: FK): string {
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
     * Low-level helper — write a single entry into this store's versionMap.
     */
    private _writeEntry(
        derivedKeyHex: string,
        value: string | Buffer | Tombstone,
        version: number,
    ): void {
        let entries = this.versionMap.get(derivedKeyHex);
        if (!entries) {
            entries = [];
            this.versionMap.set(derivedKeyHex, entries);
        }
        entries.push({ version, value });
    }
}
