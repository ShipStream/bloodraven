package pitrplan_test

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrplan"
)

// TestExamplesSpilled runs the design's worked examples (§4.15, the
// planner's oracle) again with every array of the index pass and of the
// planner in a file arena, written back every 16 pages. The examples
// index real binlog bytes through sidecar.InspectTransactions, so the
// transaction index is built in the arena too. Each example asserts its
// exact segments, sets and refusal texts: a plan that depended on where
// an array lives would fail here.
func TestExamplesSpilled(t *testing.T) {
	pitrplan.ForceSpill(t)
	for name, example := range map[string]func(*testing.T){
		"Example1SingleSite":               TestExample1SingleSite,
		"Example2PlannedFailover":          TestExample2PlannedFailover,
		"Example3FailbackWithoutReclone":   TestExample3FailbackWithoutReclone,
		"ExampleG1RecloneRecordIsNoAnchor": TestExampleG1RecloneRecordIsNoAnchor,
		"Example4FailbackWithReclone":      TestExample4FailbackWithReclone,
		"Example5LossyFailover":            TestExample5LossyFailover,
		"Example6Errant":                   TestExample6Errant,
		"Example6InjectedEmpty":            TestExample6InjectedEmpty,
		"Example7SplitBrain":               TestExample7SplitBrain,
		"Example8EmptyHead":                TestExample8EmptyHead,
		"Example9RoundTwo":                 TestExample9RoundTwo,
		"Example10CarriedOver":             TestExample10CarriedOver,
	} {
		t.Run(name, example)
	}
}
