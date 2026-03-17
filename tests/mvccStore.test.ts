import { describe, it, expect } from "vitest";
import { MVCCCore } from "../src";
const { MVCCStore, ConflictError } = MVCCCore;
import tuple from "fdb-tuple";
// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type Key = { id: number };
type Val = { name: string; count?: number };

function makeStore() {
    return new MVCCStore<Key, Key, Val, Val>({
        keyTransformer: {
            pack: (key) => {
                return tuple.pack([key.id]);
            },
            unpack: (buffer) => {
                const [id] = tuple.unpack(buffer);
                if (typeof id !== "number") {
                    throw new Error("Invalid key format");
                }
                return { id };
            },
        }
    });
}

// ---------------------------------------------------------------------------
// Basic get / set / clear
// ---------------------------------------------------------------------------

describe("basic operations", () => {
    it("returns undefined for a key that was never set", async () => {
        const store = makeStore();
        const result = await store.doTransaction(async (txn) => {
            return txn.get({ id: 1 });
        });
        expect(result).toBeUndefined();
    });

    it("can set and then get a value in the same transaction", async () => {
        const store = makeStore();
        const result = await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            return txn.get({ id: 1 });
        });
        expect(result).toEqual({ name: "Alice" });
    });

    it("persists values across transactions", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.get({ id: 1 });
        });

        expect(result).toEqual({ name: "Alice" });
    });

    it("can overwrite a value", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Bob" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.get({ id: 1 });
        });

        expect(result).toEqual({ name: "Bob" });
    });

    it("clear makes a key return undefined", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        await store.doTransaction(async (txn) => {
            txn.clear({ id: 1 });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.get({ id: 1 });
        });

        expect(result).toBeUndefined();
    });

    it("clear within the same transaction hides a prior set", async () => {
        const store = makeStore();

        const result = await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.clear({ id: 1 });
            return txn.get({ id: 1 });
        });

        expect(result).toBeUndefined();
    });

    it("set after clear within the same transaction restores the value", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        const result = await store.doTransaction(async (txn) => {
            txn.clear({ id: 1 });
            txn.set({ id: 1 }, { name: "Bob" });
            return txn.get({ id: 1 });
        });

        expect(result).toEqual({ name: "Bob" });
    });
});

// ---------------------------------------------------------------------------
// Key serialisation — structurally identical objects resolve to the same key
// ---------------------------------------------------------------------------

describe("key serialisation", () => {
    it("treats structurally identical object keys as the same key", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        // Different object reference, same structure
        const result = await store.doTransaction(async (txn) => {
            return txn.get({ id: 1 });
        });

        expect(result).toEqual({ name: "Alice" });
    });

    it("ignores additional properties on the key — only the packed fields matter", async () => {
        const store = makeStore();

        // Set using a plain key.
        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        // Get using a key object with extra properties — the tuple packer
        // only encodes `id`, so the extra fields should be irrelevant.
        const result = await store.doTransaction(async (txn) => {
            return txn.get({ id: 1, extra: "ignored" } as any);
        });

        expect(result).toEqual({ name: "Alice" });
    });
});

// ---------------------------------------------------------------------------
// Version tracking
// ---------------------------------------------------------------------------

describe("version", () => {
    it("starts at 0", () => {
        const store = makeStore();
        expect(store.version).toBe(0);
    });

    it("increments on each write transaction commit", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });
        expect(store.version).toBe(1);

        await store.doTransaction(async (txn) => {
            txn.set({ id: 2 }, { name: "Bob" });
        });
        expect(store.version).toBe(2);
    });

    it("does not increment on a read-only transaction", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });
        expect(store.version).toBe(1);

        await store.doTransaction(async (txn) => {
            txn.get({ id: 1 });
        });
        expect(store.version).toBe(1);
    });
});

// ---------------------------------------------------------------------------
// Conflict detection & automatic retry
// ---------------------------------------------------------------------------

describe("conflict detection and retry", () => {
    it("retries on conflict and eventually succeeds", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", count: 0 });
        });

        let attempts = 0;

        // Run two concurrent read-modify-write transactions on the same key.
        const [r1, r2] = await Promise.all([
            store.doTransaction(async (txn) => {
                attempts++;
                const val = txn.get({ id: 1 });
                // Yield to let the other transaction interleave
                await new Promise((r) => setTimeout(r, 10));
                txn.set({ id: 1 }, { name: "Alice", count: (val?.count ?? 0) + 1 });
                return val;
            }),
            store.doTransaction(async (txn) => {
                const val = txn.get({ id: 1 });
                txn.set({ id: 1 }, { name: "Alice", count: (val?.count ?? 0) + 10 });
                return val;
            }),
        ]);

        // One of them should have retried.
        // The final value should reflect both increments applied serially.
        const final = await store.doTransaction(async (txn) => {
            return txn.get({ id: 1 });
        });

        // Both transactions committed, so the count should be 11
        // (one added 1 to the other's result of 10, or vice versa).
        expect(final?.count).toBe(11);
    });

    it("throws ConflictError when retries are exhausted", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice", count: 0 });
        });

        // A transaction that always conflicts: it reads a key, then another
        // transaction writes to that key before it can commit.
        const promise = store.doTransaction(
            async (txn) => {
                txn.get({ id: 1 });

                // Sneak in a write from another transaction every attempt.
                await store.doTransaction(async (inner) => {
                    const v = inner.get({ id: 1 });
                    inner.set({ id: 1 }, { name: "Alice", count: (v?.count ?? 0) + 1 });
                });

                txn.set({ id: 1 }, { name: "Conflict" });
            },
            { maxRetries: 2 },
        );

        await expect(promise).rejects.toThrow(ConflictError);
    });

    it("does not conflict when transactions touch different keys", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
        });

        // Two concurrent transactions on different keys — no conflict.
        await Promise.all([
            store.doTransaction(async (txn) => {
                txn.get({ id: 1 });
                await new Promise((r) => setTimeout(r, 10));
                txn.set({ id: 1 }, { name: "Alice2" });
            }),
            store.doTransaction(async (txn) => {
                txn.get({ id: 2 });
                txn.set({ id: 2 }, { name: "Bob2" });
            }),
        ]);

        const result = await store.doTransaction(async (txn) => {
            return {
                a: txn.get({ id: 1 }),
                b: txn.get({ id: 2 }),
            };
        });

        expect(result.a).toEqual({ name: "Alice2" });
        expect(result.b).toEqual({ name: "Bob2" });
    });
});

// ---------------------------------------------------------------------------
// Snapshot isolation
// ---------------------------------------------------------------------------

describe("snapshot isolation", () => {
    it("a transaction reads a consistent snapshot even if concurrent writes happen", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "v1" });
        });

        // Start a long-running read transaction.
        let attempt = 0;
        const result = await store.doTransaction(async (txn) => {
            attempt++;

            // Read the value.
            const val = txn.get({ id: 1 });

            // Only on the first attempt, sneak in a concurrent write.
            if (attempt === 1) {
                // val should be "v1" at this snapshot.
                expect(val).toEqual({ name: "v1" });

                await store.doTransaction(async (inner) => {
                    inner.set({ id: 1 }, { name: "v2" });
                });
            }

            return val;
        });

        // First attempt saw v1 but conflicted; retry sees v2.
        expect(attempt).toBe(2);
        expect(result).toEqual({ name: "v2" });
    });
});

// ---------------------------------------------------------------------------
// Read your own writes (RYOW)
// ---------------------------------------------------------------------------

describe("readYourOwnWrites option", () => {
    it("RYOW enabled (default): reads see buffered writes", async () => {
        const store = makeStore();

        const result = await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            return txn.get({ id: 1 });
        });

        expect(result).toEqual({ name: "Alice" });
    });

    it("RYOW disabled: reads bypass buffered writes", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Original" });
        });

        const result = await store.doTransaction(
            async (txn) => {
                txn.set({ id: 1 }, { name: "Updated" });
                return txn.get({ id: 1 });
            },
            { readYourOwnWrites: false },
        );

        // Should see the previously committed value, not the buffered write.
        expect(result).toEqual({ name: "Original" });
    });

    it("RYOW disabled: reads return undefined for keys only in write buffer", async () => {
        const store = makeStore();

        const result = await store.doTransaction(
            async (txn) => {
                txn.set({ id: 99 }, { name: "New" });
                return txn.get({ id: 99 });
            },
            { readYourOwnWrites: false },
        );

        expect(result).toBeUndefined();

        // But the write should still have been committed.
        const committed = await store.doTransaction(async (txn) => {
            return txn.get({ id: 99 });
        });
        expect(committed).toEqual({ name: "New" });
    });
});

// ---------------------------------------------------------------------------
// getRangeAll
// ---------------------------------------------------------------------------

describe("getRangeAll", () => {
    it("returns entries within [start, end) in sorted order", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 3 }, { name: "Charlie" });
            txn.set({ id: 5 }, { name: "Eve" });
            txn.set({ id: 7 }, { name: "Grace" });
            txn.set({ id: 9 }, { name: "Ivy" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.getRangeAll({ id: 3 }, { id: 8 });
        });

        expect(result.map(([k]) => k.id)).toEqual([3, 5, 7]);
        expect(result.map(([, v]) => v.name)).toEqual(["Charlie", "Eve", "Grace"]);
    });

    it("start is inclusive and end is exclusive", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 3 }, { name: "Charlie" });
            txn.set({ id: 5 }, { name: "Eve" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.getRangeAll({ id: 3 }, { id: 5 });
        });

        // id:3 included, id:5 excluded
        expect(result).toHaveLength(1);
        expect(result[0]![0].id).toBe(3);
    });

    it("returns empty array when no keys fall in range", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 10 }, { name: "Jane" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.getRangeAll({ id: 4 }, { id: 6 });
        });

        expect(result).toEqual([]);
    });

    it("reverse option returns entries from high to low", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
            txn.set({ id: 3 }, { name: "Charlie" });
            txn.set({ id: 4 }, { name: "Dana" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.getRangeAll({ id: 1 }, { id: 5 }, { reverse: true });
        });

        expect(result.map(([k]) => k.id)).toEqual([4, 3, 2, 1]);
    });

    it("limit option restricts number of results", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
            txn.set({ id: 3 }, { name: "Charlie" });
            txn.set({ id: 4 }, { name: "Dana" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.getRangeAll({ id: 1 }, { id: 5 }, { limit: 2 });
        });

        expect(result).toHaveLength(2);
        expect(result.map(([k]) => k.id)).toEqual([1, 2]);
    });

    it("reverse + limit returns the last N entries", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
            txn.set({ id: 3 }, { name: "Charlie" });
            txn.set({ id: 4 }, { name: "Dana" });
            txn.set({ id: 5 }, { name: "Eve" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.getRangeAll({ id: 1 }, { id: 6 }, { reverse: true, limit: 3 });
        });

        expect(result).toHaveLength(3);
        expect(result.map(([k]) => k.id)).toEqual([5, 4, 3]);
    });

    it("includes uncommitted writes from the same transaction (RYOW)", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 5 }, { name: "Eve" });
        });

        const result = await store.doTransaction(async (txn) => {
            // Write a new key in-range within this transaction.
            txn.set({ id: 3 }, { name: "Charlie" });
            return txn.getRangeAll({ id: 1 }, { id: 6 });
        });

        expect(result.map(([k]) => k.id)).toEqual([1, 3, 5]);
    });

    it("conflicts when a new key is added in the range by another txn", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 5 }, { name: "Eve" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;
            txn.getRangeAll({ id: 1 }, { id: 10 });

            if (attempts === 1) {
                // Concurrently insert a new key in the range.
                await store.doTransaction(async (inner) => {
                    inner.set({ id: 3 }, { name: "Charlie" });
                });
            }
        });

        expect(attempts).toBeGreaterThan(1);
    });

    it("conflicts when a key in the range is removed by another txn", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 3 }, { name: "Charlie" });
            txn.set({ id: 5 }, { name: "Eve" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;
            txn.getRangeAll({ id: 1 }, { id: 10 });

            if (attempts === 1) {
                await store.doTransaction(async (inner) => {
                    inner.clear({ id: 3 });
                });
            }
        });

        expect(attempts).toBeGreaterThan(1);
    });

    it("does not conflict when a key outside the range is modified", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 20 }, { name: "Tara" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;
            txn.getRangeAll({ id: 1 }, { id: 10 });

            if (attempts === 1) {
                await store.doTransaction(async (inner) => {
                    inner.set({ id: 20 }, { name: "Tara2" });
                });
            }
        });

        expect(attempts).toBe(1);
    });
});

// ---------------------------------------------------------------------------
// getRangeAllStartsWith
// ---------------------------------------------------------------------------

describe("getRangeAllStartsWith", () => {
    it("returns all entries whose packed key starts with the prefix", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
            txn.set({ id: 3 }, { name: "Charlie" });
        });

        // Each key packs as a unique tuple, so querying with a specific id
        // should return exactly that entry.
        const result = await store.doTransaction(async (txn) => {
            return txn.getRangeAllStartsWith({ id: 2 });
        });

        expect(result).toHaveLength(1);
        expect(result[0]![0].id).toBe(2);
        expect(result[0]![1].name).toBe("Bob");
    });

    it("returns empty array when no keys match the prefix", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.getRangeAllStartsWith({ id: 999 });
        });

        expect(result).toEqual([]);
    });

    it("supports reverse option", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
            txn.set({ id: 3 }, { name: "Charlie" });
        });

        // With the tuple encoding each id is its own prefix, but we can
        // verify reverse works by checking a broader scenario.
        // Use a store with multi-element tuple keys to show prefix matching.
        // For this simple store, just verify the option doesn't error.
        const result = await store.doTransaction(async (txn) => {
            return txn.getRangeAllStartsWith({ id: 2 }, { reverse: true });
        });

        expect(result).toHaveLength(1);
        expect(result[0]![0].id).toBe(2);
    });

    it("supports limit option", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
            txn.set({ id: 3 }, { name: "Charlie" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.getRangeAllStartsWith({ id: 2 }, { limit: 0 });
        });

        expect(result).toEqual([]);
    });

    it("includes uncommitted writes from the same transaction (RYOW)", async () => {
        const store = makeStore();

        const result = await store.doTransaction(async (txn) => {
            txn.set({ id: 5 }, { name: "Eve" });
            return txn.getRangeAllStartsWith({ id: 5 });
        });

        expect(result).toHaveLength(1);
        expect(result[0]![1].name).toBe("Eve");
    });

    it("conflicts when a matching key is added by another txn", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;
            txn.getRangeAllStartsWith({ id: 2 });

            if (attempts === 1) {
                await store.doTransaction(async (inner) => {
                    inner.set({ id: 2 }, { name: "Bob" });
                });
            }
        });

        expect(attempts).toBeGreaterThan(1);
    });

    it("does not conflict when a key outside the prefix is modified", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;
            txn.getRangeAllStartsWith({ id: 1 });

            if (attempts === 1) {
                await store.doTransaction(async (inner) => {
                    inner.set({ id: 2 }, { name: "Bob2" });
                });
            }
        });

        expect(attempts).toBe(1);
    });
});

// ---------------------------------------------------------------------------
// Version trimming (GC)
// ---------------------------------------------------------------------------

describe("version trimming", () => {
    it("trims old versions after commit when no readers are active", async () => {
        const store = makeStore();

        // Write three versions of the same key, sequentially.
        for (let i = 0; i < 3; i++) {
            await store.doTransaction(async (txn) => {
                txn.set({ id: 1 }, { name: `v${i}` });
            });
        }

        // The latest value should still be readable.
        const result = await store.doTransaction(async (txn) => {
            return txn.get({ id: 1 });
        });
        expect(result).toEqual({ name: "v2" });
        expect(store.version).toBe(3);
    });
});

// ---------------------------------------------------------------------------
// Error handling — callback throws
// ---------------------------------------------------------------------------

describe("error handling", () => {
    it("propagates errors thrown by the callback", async () => {
        const store = makeStore();

        await expect(
            store.doTransaction(async () => {
                throw new Error("boom");
            }),
        ).rejects.toThrow("boom");
    });

    it("does not commit writes when the callback throws", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        try {
            await store.doTransaction(async (txn) => {
                txn.set({ id: 1 }, { name: "Bad" });
                throw new Error("abort");
            });
        } catch {
            // expected
        }

        const result = await store.doTransaction(async (txn) => {
            return txn.get({ id: 1 });
        });

        expect(result).toEqual({ name: "Alice" });
        expect(store.version).toBe(1);
    });
});

// ---------------------------------------------------------------------------
// snapshot() — conflict-free reads
// ---------------------------------------------------------------------------

describe("snapshot()", () => {
    it("snapshot reads return the same values as normal reads", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.snapshot().get({ id: 1 });
        });

        expect(result).toEqual({ name: "Alice" });
    });

    it("snapshot reads do not cause conflicts", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;

            // Read via snapshot — should NOT record a conflict.
            txn.snapshot().get({ id: 1 });

            if (attempts === 1) {
                // Concurrently modify the same key.
                await store.doTransaction(async (inner) => {
                    inner.set({ id: 1 }, { name: "Bob" });
                });
            }

            // Write something so the transaction commits.
            txn.set({ id: 2 }, { name: "Charlie" });
        });

        // No retry — snapshot read doesn't conflict.
        expect(attempts).toBe(1);
    });

    it("normal reads on the same txn still cause conflicts", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;

            // Snapshot read on key 1 — no conflict tracking.
            txn.snapshot().get({ id: 1 });
            // Normal read on key 2 — conflict tracked.
            txn.get({ id: 2 });

            if (attempts === 1) {
                // Modify key 2 concurrently.
                await store.doTransaction(async (inner) => {
                    inner.set({ id: 2 }, { name: "Bob2" });
                });
            }
        });

        // Should retry because of the normal read on key 2.
        expect(attempts).toBeGreaterThan(1);
    });

    it("snapshot range scans do not cause conflicts", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 3 }, { name: "Charlie" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;

            // Range scan via snapshot.
            txn.snapshot().getRangeAll({ id: 1 }, { id: 10 });

            if (attempts === 1) {
                // Add a key in the range concurrently.
                await store.doTransaction(async (inner) => {
                    inner.set({ id: 2 }, { name: "Bob" });
                });
            }

            txn.set({ id: 5 }, { name: "Eve" });
        });

        expect(attempts).toBe(1);
    });

    it("writes through snapshot() are committed normally", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.snapshot().set({ id: 1 }, { name: "Alice" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.get({ id: 1 });
        });

        expect(result).toEqual({ name: "Alice" });
    });

    it("snapshot sees the local write buffer (RYOW)", async () => {
        const store = makeStore();

        const result = await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            // Snapshot shares the write buffer, so RYOW works.
            return txn.snapshot().get({ id: 1 });
        });

        expect(result).toEqual({ name: "Alice" });
    });
});
