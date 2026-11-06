package main

import (
	"fmt"
	"os"
)

// runPITRArchiveCheck is `bloodraven pitr-archive-check` (#203, IMPL-203
// §4.4), the single container of a PITR archive check Job. It probes one
// archive destination with the mounted candidate key and writes the
// controller.PITRArchiveCheckLinePrefix line to the termination log, then
// exits 0; a transient store error exits non-zero without a line.
//
// Step 0 stub: WP-B implements it (with a testable core in the style of
// artifactCleanup).
func runPITRArchiveCheck(_ []string) {
	fmt.Fprintln(os.Stderr, "pitr-archive-check: not implemented")
	os.Exit(1)
}
