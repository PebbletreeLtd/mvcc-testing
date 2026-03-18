// A subspace is a wrapper around a prefix and key and value transformers. This
// is nearly equivalent to subspaces in the other bindings - the difference is
// it also includes kv transformers, so a subspace here will also automatically
// encode and decode keys and values.

import * as tuple from "fdb-tuple"
import { Transformer, VersionedEntry } from "./types"
import { OrderedMap } from "./OrderedMap"

// Template parameters refer to the types of the allowed key and values you pass
// in to the database (eg in a set(keyin, valin) call) and the types of keys and
// values returned. KeyIn == KeyOut and ValIn == ValOut in almost all cases.
export default class Subspace<KeyIn, KeyOut, ValIn, ValOut> {
    keyXf: Transformer<KeyIn, KeyOut>
    readonly versionMap?: OrderedMap<string, VersionedEntry[]>

    /**
     * Optional string prefix.  When set, it is tuple-packed and automatically
     * prepended to / stripped from keys in {@link packKey} and {@link unpackKey}.
     * {@link contains} checks whether a serialised key belongs to this subspace.
     */
    readonly prefix?: string
    private readonly _prefixBuf: Buffer
    private readonly _prefixHex: string

    constructor(keyXf: Transformer<KeyIn, KeyOut>, prefix?: string) {
        this.keyXf = keyXf
        if (prefix != null) {
            this.prefix = prefix
            this._prefixBuf = tuple.pack([prefix]) as Buffer
            this._prefixHex = this._prefixBuf.toString("hex")
        } else {
            this._prefixBuf = Buffer.alloc(0)
            this._prefixHex = ""
        }
    }

    /**
     * Returns `true` if a hex-encoded serialised key starts with this
     * subspace's prefix.  Always returns `true` when no prefix is set.
     */
    contains(serialisedKeyHex: string): boolean {
        if (!this.prefix) return true
        return serialisedKeyHex.startsWith(this._prefixHex)
    }

    // Helpers to inspect whats going on.
    packKey(key: KeyIn): string | Buffer {
        const packed = this.keyXf.pack(key)
        if (this._prefixBuf.length === 0) return packed
        return Buffer.concat([this._prefixBuf, packed])
    }
    unpackKey(key: Buffer): KeyOut {
        if (this._prefixBuf.length > 0) {
            const actual = key.subarray(0, this._prefixBuf.length)
            if (!actual.equals(this._prefixBuf)) {
                throw new Error(
                    `Key does not belong to subspace "${this.prefix}": ` +
                    `expected prefix ${this._prefixHex}, got ${actual.toString("hex")}`
                )
            }
        }
        const stripped = this._prefixBuf.length > 0
            ? key.subarray(this._prefixBuf.length)
            : key
        return this.keyXf.unpack(stripped as Buffer)
    }
    packValue(val: ValIn): string | Buffer {
        return JSON.stringify(val)
    }
    unpackValue(val: Buffer): ValOut {
        return JSON.parse(val.toString()) as ValOut
    }
    withKeyEncoding<NewKeyIn, NewKeyOut>(keyXf: Transformer<NewKeyIn, NewKeyOut>): Subspace<NewKeyIn, NewKeyOut, ValIn, ValOut> {
        return new Subspace(keyXf, this.prefix)
    }
}
