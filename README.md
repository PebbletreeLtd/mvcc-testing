# @pebbletree/mvcc-testing

An in-memory, MVCC-based transactional key/value store for integration testing. Inspired by [FoundationDB's node bindings](https://apple.github.io/foundationdb/class-scheduling-nodejs.html), it provides snapshot isolation, optimistic conflict detection, and automatic transaction retries — all without an external database.

## Installation

```bash
npm install @pebbletree/mvcc-testing
```

## Quick start

```ts
import { MVCCStore } from "@pebbletree/mvcc-testing";

type Key = { id: number };
type Val = { name: string; count: number };

const store = new MVCCStore<Key, Val>();

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

### `MVCCStore<K, V>`

The top-level store. `K` is the key type and `V` is the value type. Keys must be JSON-serialisable — they are deterministically serialised via [`json-stable-stringify`](https://github.com/ljharb/json-stable-stringify) so that structurally identical objects always resolve to the same internal map entry.

#### `store.doTransaction<R>(callback, options?): Promise<R>`

Execute `callback` inside a transaction. The callback receives a `Transaction` and should return a promise. On success the transaction is committed atomically; on conflict the callback is re-invoked from scratch (up to `maxRetries` times).

```ts
const value = await store.doTransaction(async (txn) => {
  const v = txn.get({ id: 1 });
  txn.set({ id: 1 }, { ...v, count: (v?.count ?? 0) + 1 });
  return v;
});
```

##### Options (`TransactionOptions`)

| Option | Type | Default | Description |
|---|---|---|---|
| `maxRetries` | `number` | `5` | Maximum retry attempts on conflict before throwing `ConflictError`. |
| `readYourOwnWrites` | `boolean` | `true` | When `true`, reads within the transaction see uncommitted buffered writes. Set to `false` to always read from the snapshot. |

#### `store.version: number`

The current commit version (monotonically increasing). Useful for diagnostics and testing.

---

### `Transaction<K, V>`

Provided to the `doTransaction` callback. All operations are scoped to a point-in-time snapshot.

#### `txn.get(key: K): V | undefined`

Read the value for `key`. Returns `undefined` if the key has never been set or has been cleared.

When `readYourOwnWrites` is enabled (default), buffered writes from earlier in the same transaction are visible. When disabled, reads always return the snapshot value.

#### `txn.set(key: K, value: V): void`

Buffer a write. The value is not visible to other transactions until the transaction commits.

#### `txn.clear(key: K): void`

Mark a key as deleted. After commit, `get` will return `undefined` for this key.

#### `txn.getUsingFilter(filter: (key: K, value: V) => boolean): { key: K; value: V }[]`

Scan every live key/value pair in the store and return all entries where `filter` returns `true`. This is the equivalent of a table scan with a predicate.

**Conflict tracking:**

- A **point read** is recorded for each matched row, so value changes on matched keys are detected as conflicts (same as a normal `get`).
- A **filter read** is recorded with the filter callback and the set of matched keys. At commit time the filter is re-evaluated against the current store to detect keys that were **added to** or **removed from** the result set by concurrent transactions.

```ts
const users = await store.doTransaction(async (txn) => {
  return txn.getUsingFilter((_key, val) => val.role === "admin");
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
| **Filter read** (`getUsingFilter`) | The set of keys matching the filter has changed (additions or removals). Value changes on individual matched keys are covered by the companion point reads. |

Read operations are represented as a **discriminated union** (`ReadOperation<K, V>`):

```ts
type ReadOperation<K, V> = KeyReadOperation | FilterReadOperation<K, V>;
```

The commit phase dispatches each operation through `processReadOperationForConflicts`, which applies the appropriate check based on the operation's `type` discriminant.

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
  types.ts          # TOMBSTONE, VersionedEntry, TransactionOptions,
                    # ReadOperation union, ConflictError
  Transaction.ts    # Transaction<K, V> — get, set, clear, getUsingFilter
  MVCCStore.ts      # MVCCStore<K, V> — doTransaction, conflict detection,
                    # version trimming, retry loop
  index.ts          # Barrel re-exports
tests/
  mvccStore.test.ts # Comprehensive test suite (vitest)
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
