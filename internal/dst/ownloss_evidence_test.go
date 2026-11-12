package dst

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

// scheduleGoldenBeforeOwnLossEvidenceFaults is scheduleGolden at the commit
// before the evidence faults (failedClone, silentWitness) were added.
const scheduleGoldenBeforeOwnLossEvidenceFaults = "4d76e67d4b87f6a5a7354eadf67c973a480c13f5d638f1b5c5175cba4e921f6d"

func isOwnLossEvidenceOp(op Op) bool {
	return op.Kind == OpFailedClone || op.Kind == OpSilentWitness || op.Kind == OpSilentWitnessHeal
}

// The evidence faults come from a PRNG stream of their own: with them
// taken out, every seed's schedule is what it was before they existed.
func TestGenerateTrial_OwnLossEvidenceFaultsOnlyAdd(t *testing.T) {
	h := sha256.New()
	failed, silent, heals, replace := 0, 0, 0, 0
	for seed := uint64(1); seed <= 2000; seed++ {
		tr := GenerateTrial(seed)
		kept := tr.Ops[:0:0]
		silentAt, healAt := -1, -1
		for _, op := range tr.Ops {
			switch op.Kind {
			case OpFailedClone:
				failed++
				if op.At < tr.WarmupPolls || op.At >= tr.HealAt || op.Site == "" {
					t.Fatalf("seed %d: failed clone outside its shape: %s", seed, op)
				}
				continue
			case OpSilentWitness:
				silent++
				silentAt = op.At
				replace += op.K
				if op.At < tr.WarmupPolls || op.At >= tr.HealAt || op.N < 3 || op.N > 42 || op.K < 0 || op.K > 1 || op.Site == "" {
					t.Fatalf("seed %d: silent witness outside its shape: %s", seed, op)
				}
				continue
			case OpSilentWitnessHeal:
				heals++
				healAt = op.At
				continue
			}
			kept = append(kept, op)
		}
		if healAt >= 0 && (silentAt < 0 || healAt < silentAt+3 || healAt > silentAt+42 || healAt >= tr.HealAt) {
			t.Fatalf("seed %d: silent at p%d, healed at p%d (heal-all at p%d)", seed, silentAt, healAt, tr.HealAt)
		}
		tr.Ops = kept
		fmt.Fprint(h, tr.String())
	}
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != scheduleGoldenBeforeOwnLossEvidenceFaults {
		t.Fatalf("the evidence faults changed the rest of the schedule: hash %s, want %s", got, scheduleGoldenBeforeOwnLossEvidenceFaults)
	}
	if failed < 300 || failed > 500 || silent < 400 || silent > 600 || heals < silent/4 || replace < silent/3 || replace > 2*silent/3 {
		t.Fatalf("evidence faults over 2000 seeds: %d failed clones, %d silent witnesses (%d healed before heal-all, %d with a manager replacement); want about one trial in five and one in four, half of them replacements",
			failed, silent, heals, replace)
	}
}
