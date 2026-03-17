import { describe, it, expect } from "vitest";
import { MVCCCore } from "../src";
const { MVCCStore, DerivedMVCCStore } = MVCCCore;
import tuple from "fdb-tuple";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type Key = { id: number };
type Val = { name: string; category?: string; count?: number };

/** Index key — just the category string. */
type IKey = { category: string };

/** Index value — the original id + name. */
type IVal = { id: number; name: string };

function makeStore() {
    return new MVCCStore<Key, Key, Val, Val>({
        keyTransformer: {
            pack: (key) => tuple.pack([key.id]),
            unpack: (buffer) => {
                const [id] = tuple.unpack(buffer);
                if (typeof id !== "number") throw new Error("Invalid key format");
                return { id };
            },
        },
    });
}

function makeDerived(source: InstanceType<typeof MVCCStore<Key, Key, Val, Val>>) {
    return new DerivedMVCCStore<Key, Key, Val, Val, IKey, IVal>({
        source,
        mapKey: (_key, value) => ({ category: value.category ?? "none" }),
        mapValue: (key, value) => ({ id: key.id, name: value.name }),
        keyTransformer: {
            pack: (k) => tuple.pack([k.category]),
            unpack: (buffer) => {
                const [category] = tuple.unpack(buffer);
                if (typeof category !== "string") throw new Error("Invalid key format");
                return { category };
            },
        },
    });
}

// ---------------------------------------------------------------------------
// Basic derived store behaviour
// ---------------------------------------------------------------------------

describe("DerivedMVCCStore", () => {
    it("reflects source writes as projected entries", async () => {
        const source = makeStore();
        const derived = makeDerived(source);

        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
        });

        const result = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "admin" });
        });

        expect(result).toEqual({ id: 1, name: "Alice" });
    });

    it("backfills existing source data on construction", async () => {
        const source = makeStore();

        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
            txn.set({ id: 2 }, { name: "Bob", category: "user" });
        });

        // Construct derived AFTER source already has data.
        const derived = makeDerived(source);

        const admin = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "admin" });
        });
        const user = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "user" });
        });

        expect(admin).toEqual({ id: 1, name: "Alice" });
        expect(user).toEqual({ id: 2, name: "Bob" });
    });

    it("source clear → derived key becomes undefined", async () => {
        const source = makeStore();
        const derived = makeDerived(source);

        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
        });

        await source.doTransaction(async (txn) => {
            txn.clear({ id: 1 });
        });

        const result = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "admin" });
        });

        expect(result).toBeUndefined();
    });

    it("value update that changes indexed field moves the index entry", async () => {
        const source = makeStore();
        const derived = makeDerived(source);

        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
        });

        // Change Alice's category from "admin" to "user".
        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "user" });
        });

        const oldEntry = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "admin" });
        });
        const newEntry = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "user" });
        });

        expect(oldEntry).toBeUndefined();
        expect(newEntry).toEqual({ id: 1, name: "Alice" });
    });

    it("value update that does NOT change indexed field updates in-place", async () => {
        const source = makeStore();
        const derived = makeDerived(source);

        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
        });

        // Change Alice's name but keep the category.
        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alicia", category: "admin" });
        });

        const result = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "admin" });
        });

        expect(result).toEqual({ id: 1, name: "Alicia" });
    });

    it("version tracks the source version", async () => {
        const source = makeStore();
        const derived = makeDerived(source);

        expect(derived.version).toBe(0);

        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
        });

        expect(derived.version).toBe(source.version);
        expect(derived.version).toBe(1);
    });

    it("getRangeAll works on the derived store", async () => {
        const source = makeStore();

        // Use a derived store that indexes by id (numeric as string) so we
        // can do meaningful range queries.
        type NK = { idStr: string };
        type NV = { name: string };
        const derived = new DerivedMVCCStore<Key, Key, Val, Val, NK, NV>({
            source,
            mapKey: (key) => ({ idStr: String(key.id) }),
            mapValue: (_key, value) => ({ name: value.name }),
            keyTransformer: {
                pack: (k) => tuple.pack([k.idStr]),
                unpack: (buffer) => {
                    const [idStr] = tuple.unpack(buffer);
                    return { idStr: idStr as string };
                },
            },
        });

        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
            txn.set({ id: 3 }, { name: "Charlie" });
        });

        const result = await derived.doTransaction(async (txn) => {
            return txn.getRangeAll({ idStr: "1" }, { idStr: "3" });
        });

        expect(result.map(([k]) => k.idStr)).toEqual(["1", "2"]);
    });

    it("throws when writes are attempted on the derived store", async () => {
        const source = makeStore();
        const derived = makeDerived(source);

        // clear() is callable at runtime even though set() is blocked by `never`.
        await expect(
            derived.doTransaction(async (txn) => {
                (txn as any).clear({ category: "admin" });
            }),
        ).rejects.toThrow("DerivedMVCCStore is read-only");
    });

    it("handles multiple source writes in one transaction", async () => {
        const source = makeStore();
        const derived = makeDerived(source);

        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
            txn.set({ id: 2 }, { name: "Bob", category: "user" });
            txn.set({ id: 3 }, { name: "Charlie", category: "admin" });
        });

        // "admin" index key will have the last writer win (id: 3).
        const admin = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "admin" });
        });
        const user = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "user" });
        });

        // Both id:1 and id:3 map to category "admin". The last write (id:3)
        // overwrites, so we see Charlie.
        expect(admin).toEqual({ id: 3, name: "Charlie" });
        expect(user).toEqual({ id: 2, name: "Bob" });
    });

    it("handles source clear of a key that was never set (no-op)", async () => {
        const source = makeStore();
        const derived = makeDerived(source);

        // Clearing a non-existent key shouldn't crash the derived store.
        await source.doTransaction(async (txn) => {
            txn.clear({ id: 999 });
        });

        // No derived entries should exist.
        const result = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "none" });
        });

        expect(result).toBeUndefined();
    });
});
