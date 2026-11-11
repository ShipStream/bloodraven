package pitrplan_test

import (
	"fmt"
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrplan"
)

// examples are the design's worked examples (§4.15, the planner's oracle).
var examples = map[string]func(*testing.T){
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
}

// TestExamplesSpilled runs the worked examples again with every array of
// the index pass and of the planner in a file arena, written back every 16
// pages. The examples index real binlog bytes through
// sidecar.InspectTransactions, so the transaction index is built in the
// arena too. Each example asserts its exact segments, sets and refusal
// texts: a plan that depended on where an array lives would fail here.
func TestExamplesSpilled(t *testing.T) {
	pitrplan.ForceSpill(t)
	for name, example := range examples {
		t.Run(name, example)
	}
}

// TestExamplesWithContainmentAnchors runs the worked examples with the
// containment anchor on, in memory and spilled. The planner ships with it
// off, which is how the examples run otherwise; an example that asserts the
// shipped planner's answer to a lag-window target says so itself
// (pitrplan.Shipped). No other answer may depend on the switch.
func TestExamplesWithContainmentAnchors(t *testing.T) {
	pitrplan.EnableContainmentAnchorsForTest(t)
	for _, spilled := range []bool{false, true} {
		t.Run(fmt.Sprintf("spilled=%v", spilled), func(t *testing.T) {
			if spilled {
				pitrplan.ForceSpill(t)
			}
			for name, example := range examples {
				t.Run(name, example)
			}
		})
	}
}
