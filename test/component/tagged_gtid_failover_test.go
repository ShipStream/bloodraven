package component

import "testing"

// MySQL 8.4+ prints tagged transactions in the compact
// uuid:intervals:tag:intervals form. One SET gtid_next='AUTOMATIC:<tag>'
// transaction replicates to every site, so every candidate reports it.
// Emergency failover must still pick and promote the healthy replica.
func TestFailover_CompactTaggedGTIDCandidateIsPromoted(t *testing.T) {
	const gtid = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100:app:1-5"
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: gtid}
	h := newTestHarnessWithMySQL(t, dc1, dc2)

	h.pollN(2)
	if s := h.tm.Status(); s.Sites[0].State != "writable" || s.Sites[1].State != "read-only" {
		t.Fatalf("setup: states = %s/%s, want writable/read-only", s.Sites[0].State, s.Sites[1].State)
	}

	dc1.setError(errDown)
	h.pollN(3)

	if dc2.isReadOnly() {
		t.Fatal("dc2 was not promoted: the failover picker rejected its tagged GTID set")
	}
	if got := h.dns.getLastIP(); got != "2.2.2.2" {
		t.Errorf("DNS = %q, want 2.2.2.2 after promotion", got)
	}
	if got := h.tm.Status().PromotionGtidExecuted; got != gtid {
		t.Errorf("PromotionGtidExecuted = %q, want %q", got, gtid)
	}
}
