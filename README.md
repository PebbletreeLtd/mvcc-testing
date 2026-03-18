# @pebbletree/mvcc-testing

An in-memory, MVCC-based transactional key/value store for integration testing. Inspired by [FoundationDB's node bindings](https://apple.github.io/foundationdb/class-scheduling-nodejs.html), it provides snapshot isolation, optimistic conflict detection, and automatic transaction retries — all without an external database.

## Installation

```bash
npm install @pebbletree/mvcc-testing
```

## Quick start

```ts
import { MVCCCore } from "@pebbletree/mvcc-testing";
const { Store } = MVCCCore;

type Key = { id: number };
type Val = { name: string; count: number };

const store = new Store<Key, Key, Val, Val>({
  keyTransformer: {
    pack: (k) => JSON.stringify(k),
    unpack: (buf) => JSON.parse(buf.toString()),
  },
  prefix: "myapp",   // optional — isolates keys under a namespace
});

// All reads and writes happen inside a transaction callback.
await store.doTransaction(async (txn) => {
  txn.set({ id: 1 }, { name: "Alice", count: 0 });
});

const result = await store.doTransaction(async (txn) => {
  const val = txn.get({ id: 1 });
  txn.set({ id: 1 }, { name: "Alice", count: (val?.count ?? 0) + 1 });
  return val;
});

console.log(result); // { name: "Alice", count: 0 }
```

## API

### `Subspace<KeyIn, KeyOut, ValIn, ValOut>`

Base class providing key/value codec with an optional **prefix**. Both `Store` and `DerivedSubspace` extend `Subspace`.

When a `prefix` string is supplied, it is [tuple-packed](https://github.com/nicktindall/fdb-tuple) and automatically **prepended** to every key in `packKey()` and **stripped** in `unpackKey()`. This gives each store its own isolated key namespace — the same mechanism FoundationDB uses for directory/subspace partitioning.

#### `subspace.contains(serialisedKeyHex): boolean`

Return `true` if the hex-encoded serialised key starts with this subspace's tuple-packed prefix. Always returns `true` when no prefix is set.

```ts
const sub = new Subspace(transformer, "users");
sub.contains(someHexKey); // true if the key lives under the "users" prefix
```

#### `subspace.packKey(key): string | Buffer`

Pack a key using the transformer, prepending the prefix bytes if set.

#### `subspace.unpackKey(buf): KeyOut`

Strip the prefix bytes (if set) and unpack via the transformer. **Throws** if the buffer doesn't start with the expected prefix.

#### `subspace.withKeyEncoding<NewKeyIn, NewKeyOut>(keyXf): Subspace`

Return a new `Subspace` that shares the same **prefix** but uses a different key transformer. Useful for creating partial-key subspaces for range queries — e.g. a subspace that packs only `{ at }` for range bounds but unpacks the full `[at, job_id]` tuple from stored keys.

```ts
const fullIndex = new Subspace(fullKeyTransformer, "at");

// Partial-key subspace for range queries — same prefix, narrower pack.
const rangeSubspace = fullIndex.withKeyEncoding({
  pack: (k: { at: number }) => tuple.pack([k.at]),
  unpack: (buf) => fullIndex.keyXf.unpack(buf),  // decode the full key
});

// Use in a transaction to query by range:
const results = await store.doTransaction(async (txn) => {
  return txn.at(rangeSubspace).getRangeAll({ at: 100 }, { at: 250 });
});
```

---

### `Store<Kin, KOut, Vin, VOut>`

The top-level store. Extends `Subspace`. Keys must be JSON-serialisable — they are deterministically serialised via [`json-stable-stringify`](https://github.com/ljharb/json-stable-stringify) so that structurally identical objects always resolve to the same internal map entry.

#### `store.doTransaction<R>(callback, options?): Promise<R>`

Execute `callback` inside a transaction. The callback receives a `Transaction` and should return a promise. On success the transaction is committed atomically; on conflict the callback is re-invoked from scratch (up to `maxRetries` times).

```ts
const value = await store.doTransaction(async (txn) => {
  const v = txn.get({ id: 1 });
  txn.set({ id: 1 }, { ...v, count: (v?.count ?? 0) + 1 });
  return v;
});
```

##### Constructor options

| Option | Type | Default | Description |
|---|---|---|---|
| `keyTransformer` | `Transformer<Kin, KOut>` | _(required)_ | Pack/unpack functions for the key type. |
| `prefix` | `string` | `undefined` | Optional namespace prefix. Tuple-packed and prepended to every key. |

##### Transaction options (`TransactionOptions`)

| Option | Type | Default | Description |
|---|---|---|---|
| `maxRetries` | `number` | `5` | Maximum retry attempts on conflict before throwing `ConflictError`. |
| `readYourOwnWrites` | `boolean` | `true` | When `true`, reads within the transaction see uncommitted buffered writes. Set to `false` to always read from the snapshot. |

#### `store.version: number`

The current commit version (monotonically increasing). Useful for diagnostics and testing.

---

### `Transaction<Kin, KOut, Vin, VOut>`

Provided to the `doTransaction` callback. All operations are scoped to a point-in-time snapshot.

#### `txn.get(key: Kin): VOut | undefined`

Read the value for `key`. Returns `undefined` if the key has never been set or has been cleared.

When `readYourOwnWrites` is enabled (default), buffered writes from earlier in the same transaction are visible. When disabled, reads always return the snapshot value.

#### `txn.set(key: Kin, value: Vin): void`

Buffer a write. The value is not visible to other transactions until the transaction commits.

#### `txn.clear(key: Kin): void`

Mark a key as deleted. After commit, `get` will return `undefined` for this key.

#### `txn.getRangeAll(start: Kin, end: Kin, opts?: RangeOptions): [KOut, VOut][]`

Return all key/value pairs whose key falls within the half-open interval `[start, end)` — i.e. `key >= start` and `key < end`. Keys are compared using the transformer's packed byte order, matching FoundationDB range-read semantics.

Results are returned in sorted key order by default. Use `RangeOptions` to control order and limit.

##### `RangeOptions`

| Option | Type | Default | Description |
|---|---|---|---|
| `reverse` | `boolean` | `false` | When `true`, entries are returned from high key → low key. |
| `limit` | `number` | _(all)_ | Maximum number of entries to return. Applied after sorting/reversing. |

**Conflict tracking:** a point read per matched key plus a scan read for the range. At commit time the scan is re-evaluated to detect keys **added to** or **removed from** the result set by concurrent transactions.

```ts
// Return all entries with id in [1, 10)
const page = await store.doTransaction(async (txn) => {
  return txn.getRangeAll({ id: 1 }, { id: 10 });
});

// Last 3 entries in the range, reversed
const last3 = await store.doTransaction(async (txn) => {
  return txn.getRangeAll({ id: 1 }, { id: 100 }, { reverse: true, limit: 3 });
});
```

#### `txn.getRange(start: Kin, end: Kin, opts?: RangeOptions): AsyncGenerator<[KOut, VOut]>`

Async generator form of `getRangeAll`. Same arguments and conflict tracking, but yields entries one at a time — useful for `for await...of` loops.

```ts
await store.doTransaction(async (txn) => {
  for await (const [key, val] of txn.getRange({ id: 1 }, { id: 10 })) {
    console.log(key, val);
  }
});
```

#### `txn.getRangeAllStartsWith(prefix: Kin, opts?: RangeOptions): [KOut, VOut][]`

Return all key/value pairs whose packed key begins with the packed form of `prefix`. This mirrors FoundationDB's `getRangeStartsWith` — the exclusive upper bound is computed automatically via `strinc` (increment the last non-`0xff` byte of the packed prefix).

Supports the same `RangeOptions` as `getRangeAll` (`reverse`, `limit`). Conflict tracking is identical.

```ts
// All entries whose tuple-encoded key starts with [1]
const results = await store.doTransaction(async (txn) => {
  return txn.getRangeAllStartsWith({ id: 1 });
});

// First 5 matching entries in reverse order
const last5 = await store.doTransaction(async (txn) => {
  return txn.getRangeAllStartsWith({ id: 1 }, { reverse: true, limit: 5 });
});
```

#### `txn.getRangeStartsWith(prefix: Kin, opts?: RangeOptions): AsyncGenerator<[KOut, VOut]>`

Async generator form of `getRangeAllStartsWith`. Same arguments and conflict tracking, but yields entries one at a time.

```ts
await store.doTransaction(async (txn) => {
  for await (const [key, val] of txn.getRangeStartsWith({ id: 1 })) {
    console.log(key, val);
  }
});
```

#### `txn.at(subspace): ITransaction`

Return a scoped transaction that shares this transaction's read-operations, write-buffer, and version map but uses a different `Subspace` for key/value encoding. This mirrors FoundationDB's `txn.at(subspace)` — all reads and writes through the scoped transaction participate in the same atomic commit and conflict detection.

Throws if the subspace belongs to a different store.

```ts
const orders = new Subspace<OrderKey, OrderKey, OrderVal, OrderVal>(
  orderTransformer,
  "orders",   // prefix isolates order keys from user keys
);

await store.doTransaction(async (txn) => {
  txn.set({ userId: 1 }, { name: "Alice" });
  txn.at(orders).set({ orderId: 1 }, { item: "Widget", total: 42 });
  // Both writes commit atomically under their respective prefixes.
});
```

#### `txn.snapshot(): ITransaction`

Return a snapshot view of this transaction. Reads through the returned transaction resolve values at the same snapshot version and see the same write buffer (RYOW), but they are **not** recorded as read operations — so they will never cause conflicts. Writes still go into the shared write buffer and are committed normally.

This mirrors FoundationDB's `txn.snapshot` property.

```ts
await store.doTransaction(async (txn) => {
  // This read won't cause a conflict even if key 1 is modified concurrently.
  const val = txn.snapshot().get({ id: 1 });

  // Normal reads still track conflicts as usual.
  const other = txn.get({ id: 2 });
});
```

---

### `DerivedSubspace<Kin, KOut, Vin, VOut, FKIn, FKOut extends FKIn>`

A read-only, automatically-maintained secondary index (derived view) over a `Store`. Every commit to the source store is synchronously projected through `mapKey` and written into the **source** store's `versionMap` (namespaced by the derived store's prefix). Reads go through the normal MVCC transaction path, so you get snapshot isolation and conflict detection for free.

The derived subspace extends `Subspace<FKIn, FKOut, never, unknown>` — it is **not** a separate store. Derived entries live in the source store's version map, so `txn.at(derivedSubspace)` inside a source transaction sees them naturally. The `never` value-input type makes `set()` uncallable at compile time, and a runtime guard prevents any writes that sneak past the type system. `FKOut` must extend `FKIn`.

```ts
import { MVCCCore } from "@pebbletree/mvcc-testing";
const { Store, DerivedSubspace } = MVCCCore;

type Key = { id: number };
type Val = { name: string; category: string };
type IKey = { category: string };

const source = new Store<Key, Key, Val, Val>({ keyTransformer: { ... } });

const byCategory = new DerivedSubspace<Key, Key, Val, Val, IKey, IKey>({
  source,
  mapKey: (_key, val) => ({ category: val.category }),
  prefix: "byCategory",
  keyTransformer: {
    pack: (k) => tuple.pack([k.category]),
    unpack: (buf) => ({ category: tuple.unpack(buf)[0] as string }),
  },
});

// Source writes are automatically projected.
await source.doTransaction(async (txn) => {
  txn.set({ id: 1 }, { name: "Alice", category: "admin" });
});

// Option 1: read through the derived store's own doTransaction.
const admin = await byCategory.doTransaction(async (txn) => {
  return txn.get({ category: "admin" });
});
console.log(admin); // {}

// Option 2: read through txn.at() from a source transaction.
await source.doTransaction(async (txn) => {
  const entry = txn.at(byCategory).get({ category: "admin" });
  console.log(entry); // {}
});
```

#### Constructor options

| Option | Type | Description |
|---|---|---|
| `source` | `Store<Kin, KOut, Vin, VOut>` | The source store to derive from. |
| `mapKey` | `(key: KOut, value: VOut) => FKIn` | Project a source key/value into the derived key. |
| `keyTransformer` | `Transformer<FKIn, FKOut>` | Pack/unpack for the derived key type. |
| `prefix` | `string` _(optional)_ | Namespace prefix for the derived key space. |

#### Behaviour

- **Subspace, not a store:** `DerivedSubspace` extends `Subspace`, not `Store`. Derived entries are written directly into the source store's `versionMap`, so `txn.at(derivedSubspace)` works from any source transaction.
- **Backfill:** On construction, existing source data is projected into the source's `versionMap` under the derived prefix.
- **Live updates:** A post-commit hook on the source store keeps derived entries in sync after every commit.
- **Tombstone handling:** When a source key is cleared, the derived store looks up the previous value to derive the old index key and tombstones it.
- **Key migration:** When a value update changes the derived key (e.g. a category change), the old derived key is tombstoned and the new one is written.
- **Read-only:** `doTransaction` throws if any writes are attempted.
- **Values:** The derived store doesn't carry source values — `get` and range reads return `unknown` (an empty object `{}`).
- **Reads:** All `ITransaction` read methods (`get`, `getRangeAll`, `getRangeAllStartsWith`, `getRange`, `getRangeStartsWith`) work on the derived subspace — either through `derived.doTransaction(...)` or through `txn.at(derived)` from a source transaction.
- **Prefix isolation:** When the source store uses a `prefix`, the derived store's post-commit hook uses `contains()` to skip keys belonging to other subspaces — so multiple subspaces can share a single source store safely.

---

### `store.onCommit(hook): void`

Register a callback invoked synchronously after each successful commit. The hook receives the committed write buffer and the new commit version. Used internally by `DerivedSubspace`, but available for custom use.

```ts
store.onCommit((writes, commitVersion) => {
  console.log(`Version ${commitVersion}: ${writes.size} key(s) written`);
});
```

---

### `ConflictError`

Thrown when a transaction cannot commit and all automatic retries have been exhausted. Extends `Error`.

```ts
import { ConflictError } from "@pebbletree/mvcc-testing";

try {
  await store.doTransaction(async (txn) => { /* ... */ }, { maxRetries: 0 });
} catch (err) {
  if (err instanceof ConflictError) {
    console.log("Transaction conflicted");
  }
}
```

---

### `TOMBSTONE`

A unique symbol sentinel used internally to represent a deleted key in the version history. Exported for advanced use cases but not typically needed by consumers.

## How it works

### MVCC & snapshot isolation

Every committed write creates a new **versioned entry** for the affected key. Readers see a consistent snapshot — the latest value with a version ≤ the transaction's read version. This means concurrent readers are never blocked by writers.

### Optimistic conflict detection

Transactions run optimistically with no locks. At commit time the store checks every recorded read operation:

| Read type | Conflict condition |
|---|---|
| **Point read** (`get`) | The key was written at a version newer than the transaction's snapshot. |
| **Scan read** (`getRangeAll` / `getRangeAllStartsWith`) | The set of keys matching the scan has changed (additions or removals). Value changes on individual matched keys are covered by the companion point reads. |

Read operations are represented as a **discriminated union** (`ReadOperation`):

```ts
type ReadOperation = KeyReadOperation | ScanReadOperation;
```

A `ScanReadOperation` stores the set of matched keys captured at snapshot time and a `recheck` callback that can be re-run against the current version map at commit time to detect membership changes. The commit phase dispatches each operation through `processReadOperationForConflicts`, which applies the appropriate check based on the operation's `type` discriminant.

### Automatic retry

When a conflict is detected, the transaction callback is re-executed from scratch with a fresh snapshot — no manual retry logic needed. This mirrors FoundationDB's transaction retry loop. The `maxRetries` option (default 5) caps the number of attempts before throwing `ConflictError`.

### Read your own writes

By default, reads within a transaction see buffered (uncommitted) writes made earlier in the same callback. This can be disabled per-transaction:

```ts
await store.doTransaction(async (txn) => {
  txn.set({ id: 1 }, { name: "Alice" });
  txn.get({ id: 1 }); // undefined (reads from snapshot only)
}, { readYourOwnWrites: false });
```

### Version trimming (GC)

Old version entries are automatically pruned after each successful commit. The store tracks active read versions (with reference counting) and computes a safe trim horizon — the minimum snapshot version still held by any in-flight transaction. For each key, all entries older than the latest one visible at the trim horizon are discarded.

### Synchronous commit

The conflict check and write application happen in a single synchronous block with no `await` between them. In Node.js's single-threaded event loop this guarantees atomicity — no other microtask can interleave.

## Project structure

```
src/
  types.ts              # TOMBSTONE, VersionedEntry, TransactionOptions,
                        # ReadOperation union, ConflictError, ITransaction
  OrderedMap.ts         # Sorted map — O(1) get, O(log n) insert
  Transaction.ts        # Transaction — get, set, clear, getRangeAll,
                        # getRangeAllStartsWith, getRange, getRangeStartsWith,
                        # at, snapshot
  Store.ts              # Store — doTransaction, conflict detection,
                        # version trimming, retry loop, onCommit hooks
  DerivedSubspace.ts    # DerivedSubspace — read-only secondary index
  subspace.ts           # Key/value codec base class
  index.ts              # Barrel re-exports (MVCCCore namespace)
tests/
  store.test.ts              # Core store tests (vitest)
  derivedSubspace.test.ts    # DerivedSubspace tests
  scopedTransaction.test.ts  # Scoped transaction (at) tests
```

## Development

```bash
# Install dependencies
npm install

# Type-check
npm run build

# Run tests
npm test
```

## License

MIT
