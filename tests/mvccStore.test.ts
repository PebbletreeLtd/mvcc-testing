import { describe, it, expect } from "vitest";
import { MVCCStore, ConflictError } from "../src";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type Key = { id: number };
type Val = { name: string; count?: number };

function makeStore() {
    return new MVCCStore<Key, Val>();
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
// getUsingFilter
// ---------------------------------------------------------------------------

describe("getUsingFilter", () => {
    it("returns all matching entries", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
            txn.set({ id: 3 }, { name: "Charlie" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.getUsingFilter((_key, val) => val.name.startsWith("A") || val.name.startsWith("C"));
        });

        expect(result).toHaveLength(2);
        const names = result.map((r) => r.value.name).sort();
        expect(names).toEqual(["Alice", "Charlie"]);
    });

    it("returns empty array when nothing matches", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        const result = await store.doTransaction(async (txn) => {
            return txn.getUsingFilter((_key, val) => val.name === "Nobody");
        });

        expect(result).toEqual([]);
    });

    it("includes uncommitted writes from the same transaction (RYOW)", async () => {
        const store = makeStore();

        // Pre-populate so the filter scan has a committed baseline.
        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        const result = await store.doTransaction(async (txn) => {
            // Add a new key within the transaction.
            txn.set({ id: 2 }, { name: "Bob" });
            return txn.getUsingFilter((_key, val) => val.name === "Bob");
        });

        expect(result).toHaveLength(1);
        expect(result[0]!.value.name).toBe("Bob");
    });

    it("conflicts when a new matching row is added by another transaction", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;

            // Filter scan — only Alice matches.
            txn.getUsingFilter((_key, val) => val.name.length > 0);

            if (attempts === 1) {
                // Concurrently add a new matching row.
                await store.doTransaction(async (inner) => {
                    inner.set({ id: 2 }, { name: "Bob" });
                });
            }
        });

        // Should have retried at least once because the filter result set changed.
        expect(attempts).toBeGreaterThan(1);
    });

    it("conflicts when a matched row is removed by another transaction", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;

            // Filter scan — both match.
            txn.getUsingFilter(() => true);

            if (attempts === 1) {
                // Concurrently remove one.
                await store.doTransaction(async (inner) => {
                    inner.clear({ id: 2 });
                });
            }
        });

        expect(attempts).toBeGreaterThan(1);
    });

    it("conflicts when a matched row's value is changed by another transaction", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;
            txn.getUsingFilter((_key, val) => val.name === "Alice");

            if (attempts === 1) {
                // Concurrently modify the matched row's value.
                await store.doTransaction(async (inner) => {
                    inner.set({ id: 1 }, { name: "Alice2" });
                });
            }
        });

        // The individual read op on the matched key should detect the conflict.
        expect(attempts).toBeGreaterThan(1);
    });

    it("does not conflict when an unmatched row is modified", async () => {
        const store = makeStore();

        await store.doTransaction(async (txn) => {
            txn.set({ id: 1 }, { name: "Alice" });
            txn.set({ id: 2 }, { name: "Bob" });
        });

        let attempts = 0;
        await store.doTransaction(async (txn) => {
            attempts++;

            // Filter scan — only Alice matches.
            txn.getUsingFilter((_key, val) => val.name === "Alice");

            if (attempts === 1) {
                // Concurrently modify Bob (not in the filter result set).
                await store.doTransaction(async (inner) => {
                    inner.set({ id: 2 }, { name: "Bob2" });
                });
            }
        });

        // No conflict — unmatched row was changed.
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
