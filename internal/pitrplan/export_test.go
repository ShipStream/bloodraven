package pitrplan

import "testing"

// ForceSpill makes every budget created until the test ends spill every
// array into one file arena, for the tests of package pitrplan_test.
func ForceSpill(t testing.TB) { forceSpill(t) }
