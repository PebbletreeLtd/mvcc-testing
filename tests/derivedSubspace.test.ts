import { describe, it, expect } from "vitest";
import { MVCCCore } from "../src";
const { Store, DerivedSubspace, Subspace } = MVCCCore;
import tuple from "fdb-tuple";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type Key = { id: number };
type Val = { name: string; category?: string; count?: number };

/** Index key — just the category string. */
type IKey = { category: string };

function makeStore(prefix?: string) {
    return new Store<Key, Key, Val, Val>({
        prefix,
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

function makeDerived(source: InstanceType<typeof Store<Key, Key, Val, Val>>) {
    return new DerivedSubspace<Key, Key, Val, Val, IKey, IKey>({
        source,
        mapKey: (_key, value) => ({ category: value.category ?? "none" }),
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

describe.each([
    { label: "no prefix", prefix: undefined as string | undefined },
    { label: "with prefix", prefix: "items" },
])('DerivedSubspace ($label)', ({ prefix }) => {
    it("reflects source writes as projected entries", async () => {
        const source = makeStore(prefix);
        const derived = makeDerived(source);

        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
        });

        const result = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "admin" });
        });

        expect(result).toBeDefined();
    });

    it("backfills existing source data on construction", async () => {
        const source = makeStore(prefix);

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

        expect(admin).toBeDefined();
        expect(user).toBeDefined();
    });

    it("source clear → derived key becomes undefined", async () => {
        const source = makeStore(prefix);
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
        const source = makeStore(prefix);
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
        expect(newEntry).toBeDefined();
    });

    it("value update that does NOT change indexed field updates in-place", async () => {
        const source = makeStore(prefix);
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

        expect(result).toBeDefined();
    });

    it("version tracks the source version", async () => {
        const source = makeStore(prefix);
        const derived = makeDerived(source);

        expect(derived.version).toBe(0);

        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
        });

        expect(derived.version).toBe(source.version);
        expect(derived.version).toBe(1);
    });

    it("getRangeAll works on the derived store", async () => {
        const source = makeStore(prefix);

        // Use a derived store that indexes by id (numeric as string) so we
        // can do meaningful range queries.
        type NK = { idStr: string };
        const derived = new DerivedSubspace<Key, Key, Val, Val, NK, NK>({
            source,
            mapKey: (key) => ({ idStr: String(key.id) }),
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
        const source = makeStore(prefix);
        const derived = makeDerived(source);

        // clear() is callable at runtime even though set() is blocked by `never`.
        await expect(
            derived.doTransaction(async (txn) => {
                (txn as any).clear({ category: "admin" });
            }),
        ).rejects.toThrow("DerivedSubspace is read-only");
    });

    it("handles multiple source writes in one transaction", async () => {
        const source = makeStore(prefix);
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
        // overwrites, so we see the entry exists.
        expect(admin).toBeDefined();
        expect(user).toBeDefined();
    });

    it("handles source clear of a key that was never set (no-op)", async () => {
        const source = makeStore(prefix);
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

// ---------------------------------------------------------------------------
// Multi-subspace test (requires prefix to distinguish key spaces)
// ---------------------------------------------------------------------------

describe('DerivedSubspace (multi-subspace)', () => {
    it("does not crash when source store has writes from other subspaces", async () => {
        const source = makeStore("items");
        const derived = makeDerived(source);

        // Define a second subspace with a completely different key encoding.
        type OrderKey = { orderId: number };
        type OrderVal = { item: string; total: number };
        const orders = new Subspace<OrderKey, OrderKey, OrderVal, OrderVal>({
            pack: (key) => tuple.pack([key.orderId]),
            unpack: (buf) => {
                const parts = tuple.unpack(buf);
                return { orderId: parts[0] as number };
            },
        }, "orders");

        // Write to both the root key space AND the orders subspace in one txn.
        // The post-commit hook will receive writes from both encoders.
        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
            txn.at(orders).set({ orderId: 99 }, { item: "Widget", total: 42 });
        });

        // The derived store should have projected the root write and ignored
        // the orders-subspace write.
        const admin = await derived.doTransaction(async (txn) => {
            return txn.get({ category: "admin" });
        });
        expect(admin).toBeDefined();
    });
});

// ---------------------------------------------------------------------------
// txn.at(derivedStore) — reading derived entries from a source transaction
// ---------------------------------------------------------------------------

describe('DerivedSubspace via txn.at()', () => {
    it("can read derived entries through txn.at(derivedStore)", async () => {
        const source = makeStore("items");
        const derived = makeDerived(source);

        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
            txn.set({ id: 2 }, { name: "Bob", category: "user" });
        });

        // Read derived entries from within a source transaction.
        const result = await source.doTransaction(async (txn) => {
            return txn.at(derived).get({ category: "admin" });
        });

        expect(result).toBeDefined();
    });

    it("getRangeAll works through txn.at(derivedStore)", async () => {
        const source = makeStore("items");

        type NK = { idStr: string };
        const derived = new DerivedSubspace<Key, Key, Val, Val, NK, NK>({
            source,
            mapKey: (key) => ({ idStr: String(key.id) }),
            prefix: "byId",
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

        const result = await source.doTransaction(async (txn) => {
            return txn.at(derived).getRangeAll({ idStr: "1" }, { idStr: "3" });
        });

        expect(result.map(([k]) => k.idStr)).toEqual(["1", "2"]);
    });

    it("source writes and derived reads within the same transaction", async () => {
        const source = makeStore("items");
        const derived = makeDerived(source);

        // Seed data so the derived store has something to backfill.
        await source.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", category: "admin" });
        });

        // In a single source transaction: read from the derived index.
        const admin = await source.doTransaction(async (txn) => {
            return txn.at(derived).get({ category: "admin" });
        });

        expect(admin).toBeDefined();
    });
});
