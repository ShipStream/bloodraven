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
//  4. D, the transactions committed in (maxOct(B), T) outside Λ ∪ B ∪ X,
//     must be a subset of X (D-11);
//  5. a 0-1 BFS over (file, position) states finds the minimum-segment
//     plan, and every segment start satisfies the state-match proof
//     pred(o) ∖ X = B ∪ Replayed (§4.6, §4.7).
//
// Every refusal carries a pitrarchive.Token; §4.11 fixes their precedence.
//
// Files: archive.go (inputs), index.go (FileIndex, built by
// internal/sidecar.InspectTransactions), gtidset.go, budget.go (D-42),
// frontier.go (§4.3), witness.go (§4.4, anchors), lineage.go (L1–L3, L5),
// dwindow.go (L4), search.go (§4.6), latest.go (§4.8), identity.go (§4.9:
// closure, waiver, D-51 order check), plan.go (orchestration) and
// coverage.go (§4.12). The worked examples of §4.15 are the oracle in
// examples_test.go.
package pitrplan
