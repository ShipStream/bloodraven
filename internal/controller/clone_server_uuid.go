package controller

import (
	"context"
	"fmt"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// cloneServerUUIDProbeAttempts is how many times the recipient's
// server_uuid is read after a clone before the check gives up. The read
// follows verifyCloneOutcome, which has just had an answer from the
// restarted server, so more than one attempt only covers a dropped pooled
// connection.
const cloneServerUUIDProbeAttempts = 3

// readServerUUID reads @@server_uuid of a clone recipient. It returns ""
// when the checker cannot read a server's identity (gtidSnapshotReader) or
// the read fails; the error says which.
func readServerUUID(ctx context.Context, c mysql.Checker) (string, error) {
	r, ok := c.(gtidSnapshotReader)
	if !ok {
		return "", fmt.Errorf("the checker does not read @@server_uuid")
	}
	var snap mysql.GTIDSnapshot
	err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		snap, err = r.ReadGTIDSnapshot(probeCtx)
		return err
	})
	if err != nil {
		return "", err
	}
	if snap.ServerUUID == "" {
		return "", fmt.Errorf("the server reported an empty @@server_uuid")
	}
	return snap.ServerUUID, nil
}

// cloneServerUUIDBefore reads the recipient's server_uuid before CLONE
// INSTANCE, for verifyCloneServerUUIDRotated. A failed read is logged and
// returns "": the clone goes ahead and the check after it is skipped.
func (tm *TopologyManager) cloneServerUUIDBefore(ctx context.Context, recipient mysql.Checker, site string) string {
	uuid, err := readServerUUID(ctx, recipient)
	if err != nil {
		tm.logger.Warn("could not read the recipient server_uuid before clone", "site", site, "error", err)
	}
	return uuid
}

// verifyCloneServerUUIDRotated checks, after a clone that
// verifyCloneOutcome confirmed, that the recipient came back with another
// server_uuid than before had (issue #265).
//
// The mysql container's launcher (mysqlLauncher) removes auto.cnf on the
// start that installs a clone, so the recipient gets a new server_uuid
// and cannot reissue a GTID its discarded history already used. Nothing
// in the operator can make that happen: the launcher runs inside the pod,
// between the clone and mysqld. This is the check that a launcher which
// did not run (a pod rendered before it, an image whose mysqld is not
// started through it) or could not remove the file is seen, not silently
// tolerated.
//
// An unchanged server_uuid is reported and nothing else: a WARN line and,
// through CloneServerUUIDNotRotatedCallback, a Warning Event. It does not
// fail the bootstrap and does not make the site ineligible for promotion.
// Replication is still set up, because the recipient's data is correct and
// a replica that is not replicating is the larger risk. Promotion is not
// refused, because the hazard is to the PITR archive and only if the site
// is promoted after losing transactions, and that case already fails
// closed where it matters (the sidecar's reissue verdict,
// PITRArchiveReissueRiskDetected at the promotion, PITRArchiveHistoryReset
// on restores); refusing the site as a failover candidate would turn a
// recoverable archive problem into an outage of a two-site group.
func (tm *TopologyManager) verifyCloneServerUUIDRotated(ctx context.Context, recipient mysql.Checker, site, before string) {
	if before == "" {
		tm.logger.Info("clone recipient server_uuid rotation not verified", "site", site,
			"reason", "the recipient's server_uuid before the clone is unknown")
		return
	}
	var after string
	var err error
	for attempt := 0; attempt < cloneServerUUIDProbeAttempts; attempt++ {
		if after, err = readServerUUID(ctx, recipient); err == nil || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		tm.logger.Warn("clone recipient server_uuid rotation not verified", "site", site,
			"reason", fmt.Sprintf("could not read the recipient's server_uuid after the clone: %v", err))
		return
	}
	if after != before {
		tm.logger.Info("clone recipient server_uuid rotated", "site", site, "previousServerUuid", before, "serverUuid", after)
		return
	}
	tm.logger.Warn("clone recipient kept its server_uuid", "site", site, "serverUuid", after)
	if tm.CloneServerUUIDNotRotatedCallback != nil {
		tm.CloneServerUUIDNotRotatedCallback(site, after)
	}
}
