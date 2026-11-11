package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// cloneNotRotatedEventPrefix is how the operator's
// CloneServerUUIDNotRotated Event message starts, up to the site name
// (emitCloneServerUUIDNotRotated). TestCloneIdentityMirrors pins it.
const cloneNotRotatedEventPrefix = "site %s was cloned and kept its server_uuid"

// cloneIdentityWait bounds the read of a recloned site's server_uuid: the
// site already replicates when the check runs, so this only covers a
// port-forward to a pod that is still coming up.
const cloneIdentityWait = 2 * time.Minute

// cloneIdentity is a site's server_uuid from before a clone, for the check
// after it (#265): the mysql container's launcher gives every CLONE
// recipient a new server_uuid on the restart that installs the clone, so a
// recloned former primary can never reissue a GTID of transactions it lost.
type cloneIdentity struct {
	site string
	// before is the site's @@server_uuid before the clone was requested.
	before string
	// since is the runner's clock just before that read; an Event about
	// this clone is newer.
	since time.Time
}

// siteServerUUID reads site's @@global.server_uuid over a fresh connection
// (a clone restarts mysqld, and a pod roll replaces the pod, so a cached
// client's port-forward may be gone).
func siteServerUUID(ctx context.Context, env *runner.Env, site string) (string, error) {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return "", err
	}
	defer func() { _ = c.Close() }()
	uuid, err := c.ScalarString(ctx, "SELECT @@global.server_uuid")
	if err != nil {
		return "", fmt.Errorf("read server_uuid on %s: %w", site, err)
	}
	if uuid == "" {
		return "", fmt.Errorf("%s reports an empty server_uuid", site)
	}
	return uuid, nil
}

// startCloneIdentity records site's server_uuid before a clone of it is
// requested.
func startCloneIdentity(ctx context.Context, env *runner.Env, site string) (cloneIdentity, error) {
	since := time.Now()
	uuid, err := siteServerUUID(ctx, env, site)
	if err != nil {
		return cloneIdentity{}, fmt.Errorf("before the clone of %s: %w", site, err)
	}
	return cloneIdentity{site: site, before: uuid, since: since}, nil
}

// cloneIdentityVerdict is the rule: a completed clone leaves the recipient
// with another server_uuid than it had.
func cloneIdentityVerdict(site, before, after string) error {
	if before == "" || after == "" {
		return fmt.Errorf("clone of %s: server_uuid before=%q after=%q; both must be read to tell whether it was rotated", site, before, after)
	}
	if before == after {
		return fmt.Errorf("the clone of %s left its server_uuid %s unchanged: the mysql container's launcher did not rotate it "+
			"(its lines in the mysql container's log start with \"bloodraven-launcher:\"); a recloned former primary that is promoted again "+
			"would reissue the GTIDs of transactions it lost", site, after)
	}
	return nil
}

// assertRotated is the check after a completed clone of the site: it
// answers with another server_uuid than before, and the operator, which
// makes the same comparison, raised no CloneServerUUIDNotRotated Event for
// it. It returns the new server_uuid.
func (c cloneIdentity) assertRotated(ctx context.Context, env *runner.Env) (string, error) {
	after, err := c.read(ctx, env)
	if err != nil {
		return "", fmt.Errorf("after the clone of %s: %w", c.site, err)
	}
	if err := cloneIdentityVerdict(c.site, c.before, after); err != nil {
		return "", err
	}
	ev, found, err := findMFGEvent(ctx, env, c.since, v1alpha1.EventCloneServerUUIDNotRotated, fmt.Sprintf(cloneNotRotatedEventPrefix, c.site))
	if err != nil {
		return "", fmt.Errorf("list events after the clone of %s: %w", c.site, err)
	}
	if found {
		return "", fmt.Errorf("the operator reported the clone of %s as not rotated although its server_uuid changed %s -> %s: %s", c.site, c.before, after, ev.Message)
	}
	env.Capture.Note(fmt.Sprintf("clone of %s: server_uuid %s -> %s; no %s Event", c.site, c.before, after, v1alpha1.EventCloneServerUUIDNotRotated))
	return after, nil
}

// assertKept is the check after a clone of the site that did not complete
// and was rolled back without a mysqld restart (a killed CLONE session):
// nothing was installed, so the site still has the server_uuid from
// before. The retry that completes rotates it.
func (c cloneIdentity) assertKept(ctx context.Context, env *runner.Env) error {
	after, err := c.read(ctx, env)
	if err != nil {
		return fmt.Errorf("after the failed clone of %s: %w", c.site, err)
	}
	if after != c.before {
		return fmt.Errorf("the failed clone of %s changed its server_uuid %s -> %s although nothing was installed", c.site, c.before, after)
	}
	env.Capture.Note(fmt.Sprintf("failed clone of %s: server_uuid still %s", c.site, after))
	return nil
}

// read reads the site's server_uuid, retrying while the site does not
// answer.
func (c cloneIdentity) read(ctx context.Context, env *runner.Env) (string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, cloneIdentityWait)
	defer cancel()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		uuid, err := siteServerUUID(waitCtx, env, c.site)
		if err == nil {
			return uuid, nil
		}
		select {
		case <-waitCtx.Done():
			return "", fmt.Errorf("%s did not answer within %s: %w (last: %v)", c.site, cloneIdentityWait, waitCtx.Err(), err)
		case <-tick.C:
		}
	}
}
