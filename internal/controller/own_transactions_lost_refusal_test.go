package controller

import "testing"

// A promotion refusal recorded for a site goes with the site's report, so
// a later refusal of the same site is a change the runner announces again.
func TestOwnLoss_RefusalIsForgottenWithTheReport(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
	f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	f.tm.refuseOwnLossPromotion([]string{"dc2"})
	if got := f.tm.buildSnapshot(nil).OwnLossPromotionRefused; len(got) != 1 {
		t.Fatalf("refused = %v, want [dc2]", got)
	}
	f.tm.clearOwnLossAfterClone("dc2")
	if got := f.tm.buildSnapshot(nil).OwnLossPromotionRefused; len(got) != 0 {
		t.Fatalf("refused = %v after the report was cleared, want none", got)
	}
}
