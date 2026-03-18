import { describe, it, expect } from "vitest";
import { MVCCCore } from "../src";
const { Store, Subspace, ConflictError } = MVCCCore;
import tuple from "fdb-tuple";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type UserKey = { userId: number };
type UserVal = { name: string };

type OrderKey = { orderId: number };
type OrderVal = { item: string; total: number };

/** Store whose root key space is prefixed with "users". */
function makeStore() {
    return new Store<UserKey, UserKey, UserVal, UserVal>({
        prefix: "users",
        keyTransformer: {
            pack: (key) => tuple.pack([key.userId]),
            unpack: (buf) => {
                const parts = tuple.unpack(buf);
                return { userId: parts[0] as number };
            },
        },
    });
}

/** A plain Subspace (no store) for an "orders" namespace. */
function makeOrderSubspace() {
    return new Subspace<OrderKey, OrderKey, OrderVal, OrderVal>({
        pack: (key) => tuple.pack([key.orderId]),
        unpack: (buf) => {
            const parts = tuple.unpack(buf);
            return { orderId: parts[0] as number };
        },
    }, "orders");
}

// ---------------------------------------------------------------------------
// Scoped transactions via at()
// ---------------------------------------------------------------------------

describe("Transaction.at() — scoped transactions", () => {
    it("writes through at() are committed with the transaction", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        await store.doTransaction(async (txn) => {
            const orderTxn = txn.at(orders);
            orderTxn.set({ orderId: 1 }, { item: "Widget", total: 42 });
        });

        // Read back through a new transaction scoped to orders.
        const result = await store.doTransaction(async (txn) => {
            const orderTxn = txn.at(orders);
            return orderTxn.get({ orderId: 1 });
        });

        expect(result).toEqual({ item: "Widget", total: 42 });
    });

    it("reads through at() see committed data", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        // Write an order via at().
        await store.doTransaction(async (txn) => {
            txn.at(orders).set({ orderId: 10 }, { item: "Gadget", total: 99 });
        });

        // Read back.
        const result = await store.doTransaction(async (txn) => {
            return txn.at(orders).get({ orderId: 10 });
        });

        expect(result).toEqual({ item: "Gadget", total: 99 });
    });

    it("RYOW works within a scoped transaction", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        const result = await store.doTransaction(async (txn) => {
            const orderTxn = txn.at(orders);
            orderTxn.set({ orderId: 1 }, { item: "Widget", total: 10 });
            return orderTxn.get({ orderId: 1 });
        });

        expect(result).toEqual({ item: "Widget", total: 10 });
    });

    it("root and scoped writes are committed atomically", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        await store.doTransaction(async (txn) => {
            txn.set({ userId: 1 }, { name: "Alice" });
            txn.at(orders).set({ orderId: 1 }, { item: "Widget", total: 42 });
        });

        // A single commit version bump covers both writes.
        expect(store.version).toBe(1);

        const [user, order] = await store.doTransaction(async (txn) => {
            return [
                txn.get({ userId: 1 }),
                txn.at(orders).get({ orderId: 1 }),
            ] as const;
        });

        expect(user).toEqual({ name: "Alice" });
        expect(order).toEqual({ item: "Widget", total: 42 });
    });

    it("conflicts are detected on scoped reads", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        // Seed an order.
        await store.doTransaction(async (txn) => {
            txn.at(orders).set({ orderId: 1 }, { item: "Widget", total: 10 });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;

            // Read via scoped txn — records a read op.
            txn.at(orders).get({ orderId: 1 });

            if (attempts === 1) {
                // Concurrently modify the same key.
                await store.doTransaction(async (inner) => {
                    inner.at(orders).set({ orderId: 1 }, { item: "Widget", total: 20 });
                });
            }
        });

        // Should have retried because the scoped read conflicted.
        expect(attempts).toBeGreaterThan(1);
    });

    it("scoped range scan conflicts when the range changes", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        await store.doTransaction(async (txn) => {
            txn.at(orders).set({ orderId: 1 }, { item: "A", total: 1 });
            txn.at(orders).set({ orderId: 5 }, { item: "E", total: 5 });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;

            // Range scan on orders.
            txn.at(orders).getRangeAll({ orderId: 1 }, { orderId: 10 });

            if (attempts === 1) {
                // Concurrently add a new order in range.
                await store.doTransaction(async (inner) => {
                    inner.at(orders).set({ orderId: 3 }, { item: "C", total: 3 });
                });
            }
        });

        expect(attempts).toBeGreaterThan(1);
    });

    it("no conflict when scoped read and root write touch different keys", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        // Seed data.
        await store.doTransaction(async (txn) => {
            txn.set({ userId: 1 }, { name: "Alice" });
            txn.at(orders).set({ orderId: 1 }, { item: "Widget", total: 10 });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;

            // Read from orders namespace.
            txn.at(orders).get({ orderId: 1 });

            if (attempts === 1) {
                // Concurrently modify users namespace — different key space.
                await store.doTransaction(async (inner) => {
                    inner.set({ userId: 1 }, { name: "Alice2" });
                });
            }
        });

        // No conflict — different serialised keys.
        expect(attempts).toBe(1);
    });

    it("clear through at() removes the key", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        await store.doTransaction(async (txn) => {
            txn.at(orders).set({ orderId: 1 }, { item: "Widget", total: 10 });
        });

        await store.doTransaction(async (txn) => {
            txn.at(orders).clear({ orderId: 1 });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.at(orders).get({ orderId: 1 });
        });

        expect(result).toBeUndefined();
    });

    it("throws when at() is called with a subspace from a different store", async () => {
        const storeA = makeStore();
        const storeB = makeStore();

        await expect(
            storeA.doTransaction(async (txn) => {
                // storeB extends Subspace and has its own versionMap — should throw.
                txn.at(storeB as any);
            }),
        ).rejects.toThrow("at() requires a subspace backed by the same store");
    });

    it("getRangeAllStartsWith works through at()", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        await store.doTransaction(async (txn) => {
            txn.at(orders).set({ orderId: 1 }, { item: "A", total: 1 });
            txn.at(orders).set({ orderId: 2 }, { item: "B", total: 2 });
            txn.at(orders).set({ orderId: 3 }, { item: "C", total: 3 });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.at(orders).getRangeAllStartsWith({ orderId: 2 });
        });

        expect(result).toHaveLength(1);
        expect(result[0]![0].orderId).toBe(2);
        expect(result[0]![1].item).toBe("B");
    });

    it("multiple at() calls on the same txn share state", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        const result = await store.doTransaction(async (txn) => {
            // Write through one at() call.
            txn.at(orders).set({ orderId: 1 }, { item: "Widget", total: 10 });

            // Read through a separate at() call — should see the write (RYOW).
            return txn.at(orders).get({ orderId: 1 });
        });

        expect(result).toEqual({ item: "Widget", total: 10 });
    });
});

// ---------------------------------------------------------------------------
// withKeyEncoding
// ---------------------------------------------------------------------------

describe("Subspace.withKeyEncoding()", () => {
    it("creates a new subspace with the same prefix but different key transformer", () => {
        const original = new Subspace<UserKey, UserKey, UserVal, UserVal>({
            pack: (key) => tuple.pack([key.userId]),
            unpack: (buf) => {
                const [userId] = tuple.unpack(buf);
                return { userId: userId as number };
            },
        }, "users");

        // New subspace that packs a string key instead.
        const reKeyed = original.withKeyEncoding<string, string>({
            pack: (k) => tuple.pack([k]),
            unpack: (buf) => tuple.unpack(buf)[0] as string,
        });

        expect(reKeyed.prefix).toBe("users");
        // Pack should use the new transformer.
        const packed = reKeyed.packKey("hello") as Buffer;
        // Should start with the same prefix bytes.
        expect(original.contains(packed.toString("hex"))).toBe(true);
    });

    it("withKeyEncoding subspace works with txn.at() for range queries", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        // Seed some orders.
        await store.doTransaction(async (txn) => {
            txn.at(orders).set({ orderId: 1 }, { item: "A", total: 10 });
            txn.at(orders).set({ orderId: 5 }, { item: "E", total: 50 });
            txn.at(orders).set({ orderId: 10 }, { item: "J", total: 100 });
        });

        // Create a partial-key subspace via withKeyEncoding — packs just a
        // number for the range bound, unpacks the full OrderKey.
        const orderRange = orders.withKeyEncoding<number, OrderKey>({
            pack: (n) => tuple.pack([n]),
            unpack: (buf) => {
                const [orderId] = tuple.unpack(buf);
                return { orderId: orderId as number };
            },
        });

        const results = await store.doTransaction(async (txn) => {
            return txn.at(orderRange).getRangeAll(1, 6);
        });

        expect(results).toHaveLength(2);
        expect(results[0]![0].orderId).toBe(1);
        expect(results[1]![0].orderId).toBe(5);
    });

    it("withKeyEncoding preserves value encoding", async () => {
        const store = makeStore();
        const orders = makeOrderSubspace();

        await store.doTransaction(async (txn) => {
            txn.at(orders).set({ orderId: 42 }, { item: "Widget", total: 99 });
        });

        // Read back through withKeyEncoding — values should still decode.
        const orderRange = orders.withKeyEncoding<number, OrderKey>({
            pack: (n) => tuple.pack([n]),
            unpack: (buf) => {
                const [orderId] = tuple.unpack(buf);
                return { orderId: orderId as number };
            },
        });

        const results = await store.doTransaction(async (txn) => {
            return txn.at(orderRange).getRangeAllStartsWith(42);
        });

        expect(results).toHaveLength(1);
        expect(results[0]![0].orderId).toBe(42);
        expect(results[0]![1]).toEqual({ item: "Widget", total: 99 });
    });
});
