// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

// .............................................................................
// Ticket 3 of the fs-agent 0.2 port (see `mongo-edit-chain.ts`, `mongo-receive.ts`):
// the concrete mechanism behind `MongoReceiveDeps.applyBehind` — writing a
// `behind` head's content into the real Mongo collection, deleting what it
// lacks.
//
// fs-agent's own `_applyBehind` reconstructs the target's FULL file map from
// its `treeRef` in one step, because its content store is addressed by that
// tree. Mongo's chain entries carry only an incremental diff per entry
// (`changed`/`removed`), so reaching a head several entries ahead means
// replaying every entry between the node's own head and that head, in chain
// order — `MongoEditChain.path()` is what resolves that order.
//
// Still standalone: nothing here is wired into `MongoEditSync` yet. The real
// `MongoReceiveDeps` implementation that closes over a running sync engine's
// own head and collection handle is the next stage.
// .............................................................................

import type { Collection, Document } from 'mongodb';
import { ObjectId } from 'mongodb';

import type { MongoEditChain } from './mongo-edit-chain.ts';

/**
 * Candidate typed `_id`s for a sliceId (the string; a 24-hex ObjectId; an
 * all-digit number) so a delete resolves the common CARAT `_id` shapes
 * (ObjectId / Int32 / string). Over-matching only ever replays a harmless
 * no-op delete.
 *
 * Mirrors `MongoEditSync`'s private `_typedIdCandidates` — duplicated rather
 * than imported so this standalone module stays provably clear of the old
 * model; the two unify when the old model is retired.
 * @param sliceId - The stringified `_id`.
 * @returns The candidate typed `_id`s.
 */
export const typedIdCandidates = (sliceId: string): unknown[] => {
  const out: unknown[] = [sliceId];
  if (/^[0-9a-fA-F]{24}$/.test(sliceId) && ObjectId.isValid(sliceId)) {
    out.push(new ObjectId(sliceId));
  }
  if (/^-?\d{1,15}$/.test(sliceId)) out.push(Number(sliceId));
  return out;
};

/** The slice of the Mongo driver {@link applyBehind} needs. */
export type ApplyBehindTarget = Pick<
  Collection<Document>,
  'replaceOne' | 'deleteOne'
>;

/**
 * Writes a `behind` head's content into the collection, deleting what it
 * lacks, by replaying every chain entry between the node's own head and that
 * head — the mechanism behind `MongoReceiveDeps.applyBehind`.
 *
 * Replays in chain order so a later entry's `changed`/`removed` for the same
 * id is what the collection ends up holding, exactly as the chain's own
 * history says it should. Refuses rather than partially apply: a component
 * this node cannot read is never silently skipped.
 * @param chain - The collection's chain.
 * @param collection - The collection name (the chain's own key for it).
 * @param target - The Mongo collection to write into.
 * @param ours - This node's own head.
 * @param theirs - The head that arrived (already known to be `behind`).
 * @param maxWalk - Give up past this many entries; see {@link MongoEditChain.path}.
 * @returns `applied`, or `refused` when the path or a component could not be
 *   read — nothing was written then.
 */
export const applyBehind = async (
  chain: MongoEditChain,
  collection: string,
  target: ApplyBehindTarget,
  ours: string,
  theirs: string,
  maxWalk?: number,
): Promise<'applied' | 'refused'> => {
  const steps = await chain.path(collection, ours, theirs, maxWalk);
  if (!steps) return 'refused';

  for (const entry of steps) {
    for (const hash of Object.values(entry.changed)) {
      const doc = await chain.getComponent(collection, hash);
      if (!doc) return 'refused';
      await target.replaceOne({ _id: doc['_id'] } as never, doc, {
        upsert: true,
      } as never);
    }
    for (const sliceId of entry.removed) {
      for (const id of typedIdCandidates(sliceId)) {
        await target.deleteOne({ _id: id } as never);
      }
    }
  }
  return 'applied';
};
