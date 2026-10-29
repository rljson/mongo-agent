<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# @rljson/mongo-agent

Todo: Add description here

## Example

[src/example.ts](src/example.ts)

## Mass-delete guard

`MongoEditSync` refuses a delete burst that crosses a configurable
fraction/absolute cap of a collection's size. A refused delete is simply
never tombstoned, so anti-entropy converges the document back in from a
peer that still has it — it is not retried by the guard itself.

The guard holds this against a trailing time window of ids it has already
allowed (`SL_EDIT_DELETE_WINDOW_MS`, default `60000`), not the whole
collection, so a legitimate, unrelated delete is judged on its own merits
once the window has passed, rather than held forever for sharing a
collection with an earlier refusal. The window bounds the RATE, not the
total: a deletion slow enough to stay under the limit within any one window
still eventually propagates in full.

A refused burst raises a `deletion/refused` signal
(`action: 'required'`, `decidedBy: 'guard'`) instead of only a trace-log
line:

```ts
const sync = new MongoEditSync(
  mongoDb,
  rljsonDb,
  connector,
  collections,
  prefix,
  undefined, // discover
  undefined, // checkpoint
  undefined, // shouldSync
  (signal) => {
    // signal.kind === 'deletion/refused'
  },
);

sync.signals(); // every signal kept, oldest first
sync.signalsNeedingAction(); // only the ones a person still has to act on
sync.signalTotals(); // counts per kind, dropped ones included
```

A deliberate, announced wipe (`sync.expectMassDelete(collection)`) is
excluded from the window — it is not part of any drip, and counting it would
refuse every ordinary delete in that collection for the rest of the window.
