package controller

import (
	"log/slog"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// plannedDrainRunner wires a real TopologyManager over the iad/pdx sites of
// plannedFailoverFG into a TopologyManagerRunner.
func plannedDrainRunner(nn types.NamespacedName, iad, pdx *mockMySQL) *TopologyManagerRunner {
	cfg := TopologyConfig{
		Name: "lion",
		Sites: []SiteTopologyConfig{
			{Name: "iad", Zone: "lion-iad", LBIP: "1.1.1.1", Role: state.SiteRolePrimaryCandidate, TaintSelector: taintSelector("iad"), Host: "mysql-iad"},
			{Name: "pdx", Zone: "lion-pdx", LBIP: "2.2.2.2", Role: state.SiteRolePrimaryCandidate, TaintSelector: taintSelector("pdx"), Host: "mysql-pdx"},
		},
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
	}
	tm := NewTopologyManager(cfg, []mysql.Checker{iad, pdx},
		NewFailoverController(testLogger()), nil, nil, BootstrapConfig{},
		newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	runner := &TopologyManagerRunner{
		logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		managers: make(map[types.NamespacedName]*managedTopology),
	}
	runner.SetManagerForTest(nn, tm)
	return runner
}
