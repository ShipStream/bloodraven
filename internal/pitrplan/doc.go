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
// Series 1 of #206 ships only the coverage.json contract (coverage.go);
// the planner itself lands in series 2 (WP-B1).
package pitrplan
