// A subspace is a wrapper around a prefix and key and value transformers. This
// is nearly equivalent to subspaces in the other bindings - the difference is
// it also includes kv transformers, so a subspace here will also automatically
// encode and decode keys and values.

import { Transformer, VersionedEntry } from "./types"
import { OrderedMap } from "./OrderedMap"

// Template parameters refer to the types of the allowed key and values you pass
// in to the database (eg in a set(keyin, valin) call) and the types of keys and
// values returned. KeyIn == KeyOut and ValIn == ValOut in almost all cases.
export default class Subspace<KeyIn, KeyOut, ValIn, ValOut> {
    keyXf: Transformer<KeyIn, KeyOut>
    readonly versionMap?: OrderedMap<string, VersionedEntry[]>
    constructor(keyXf: Transformer<KeyIn, KeyOut>) {
        // Ugh typing this is a mess. Usually this will be fine since if you say new
        // Subspace() you'll get the default values for KI/KO/VI/VO.
        this.keyXf = keyXf
    }

    // Helpers to inspect whats going on.
    packKey(key: KeyIn): string | Buffer {
        return this.keyXf.pack(key)
    }
    unpackKey(key: Buffer): KeyOut {
        return this.keyXf.unpack(key)
    }
    packValue(val: ValIn): string | Buffer {
        return JSON.stringify(val)
    }
    unpackValue(val: Buffer): ValOut {
        return JSON.parse(val.toString()) as ValOut
    }
}
