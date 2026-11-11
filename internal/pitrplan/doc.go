// Package pitrplan is the pure point-in-time replay planner (design WS-D §4,
// D-38). It has no I/O: cmd/bloodraven's pitr-download reads the dump
// metadata (backupmeta.DumpMeta), opens and verifies the archive
// (pitrarchive), downloads and indexes binlog files, and hands the planner
// per-file transaction indexes. The planner returns a Plan, which the glue
// stages and records as coverage.json version 2 (Coverage, §4.12) for
// restore.py to re-validate with the server's own GTID functions.
//
// The planner, in order (§1.1):
//
//  1. B is the dump's @.json gtidExecuted;
//  2. the B-frontier pass proves maxOct(B) from bytes and checks the dump
//     against the target T and against GTID reissue (§4.3);
//  3. the witness w is the earliest archived transaction committed at or
//     after T outside B ∪ X, and Λ is w's ground-truth predecessor set
//     (§4.4), or the unique archive head in latest mode (§4.8);
//  4. D, the archived transactions committed in (maxOct(B), T) outside
//     Λ ∪ B, must be a subset of X (D-11); coverage.json reports D with
//     its excluded members;
//  5. a 0-1 BFS over (file, position) states finds the minimum-segment
//     plan, and every segment start satisfies the state-match proof
//     pred(o) ∖ X = B ∪ Replayed (§4.6, §4.7).
//
// Every refusal carries a pitrarchive.Token; §4.11 fixes their precedence.
//
// A witness archived only as a replica copy is accepted through an anchor
// (witness.go): a set its origin server provably executed before it. A
// promotion record of the origin anchors by time, with its gtidExecuted,
// when it lacks the witness and was observed more than δ before the
// witness committed. The promotion record of the witness's own episode
// anchors by containment (issue #269), with its promotion set
// (promotionGtidExecuted, which the sidecar derives from its last
// read-only observation and the binlog file opened before the episode's
// first transaction), when that set lacks the witness and the record's
// observation or its confirmation holds it: no clock has to order the
// record and the witness, which covers the transactions a new primary
// commits before and shortly after its record is observed. An archived
// origin file of the same server anchors too. Every anchor is fenced by
// the records around it (fenceOf), and exclusions never set a record
// aside (N1). A record's readOnlyGtidExecuted is not read.
//
// An anchor proves that the origin had executed a set before the witness;
// the plan certifies everything the copy follows. So an accepted copy is
// held to its anchor (copyAgainstAnchor): it follows all of the anchor,
// and the transactions it follows under the origin's server_uuid outside
// a promotion set of the origin were committed in that record's episode
// or later, by their archived copies' commit times. A copy on a server
// that lost transactions and never got them back, or on a replica that
// kept transactions its source lost and issued again, refuses.
//
// Every downloaded copy of a GTID is compared with the others as it arrives
// (ingest and compare, plan.go): commit time, origin server, emptiness, and
// its row changes (EffectDigest, issue #268). Two copies that change other
// tables refuse; two that change the same tables with the same layout and
// other row bytes refuse; a copy without a digest, or with another layout
// (the sites' table definitions or row images differ), says nothing. That
// relation is not transitive, so the occurrence registry keeps one copy of
// a GTID per layout (occ.go) and every comparable pair is compared,
// whatever the order of the downloads. Each segment records the file
// offset of its first replayed transaction (Segment.StartPosition, issue
// #271), which restore.py hands to mysqlbinlog as --start-position.
//
// Memory (D-42, issue #266). Everything whose size grows with the archive
// is charged to a Budget before it is allocated, and planning refuses
// PITRPlanTooLarge instead of being OOM-killed. What grows with the
// number of transactions is kept in arrays of fixed-size records without
// pointers, allocated through the budget (store.go): the per-file
// transaction index, the occurrence registry (occ.go), the search's
// positions, classes, keys and queues, and the order check's graph. With
// a spill arena (Budget.SetSpill; pitr-download provides one backed by
// unlinked files on the staging volume, internal/pitrplan/spillfile) these
// arrays leave the heap once the memory charge passes a threshold, so a
// large window costs staging disk instead of refusing. The planner runs
// the same code over an array wherever it lives, and a plan is the same
// byte for byte with and without a spill: TestPlansIdenticalSpilled,
// TestOraclesSpilled and TestExamplesSpilled hold it to that. GTID
// interval sets, manifest rows and the event being decoded stay in
// memory. The planner still does no I/O of its own: the arena is an
// interface, and the pages it writes are written back in bounded steps
// through it (store.go has why).
//
// Files: archive.go (inputs), index.go (FileIndex, built by
// internal/sidecar.InspectTransactions), gtidset.go, budget.go (D-42),
// store.go (the arrays and the spill), occ.go (the occurrence registry),
// frontier.go (§4.3), witness.go (§4.4, anchors), lineage.go (L1–L3, L5),
// dwindow.go (L4), search.go (§4.6), latest.go (§4.8, and the segment
// construction both modes share), identity.go (§4.9: closure, waiver, D-51
// order check), plan.go (orchestration) and
// coverage.go (§4.12). The worked examples of §4.15 are the oracle in
// examples_test.go.
package pitrplan
