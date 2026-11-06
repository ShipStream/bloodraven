package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/util"
)

// pitrArchiveCheckTerminationLogPath is where the verdict line goes.
var pitrArchiveCheckTerminationLogPath = "/dev/termination-log"

// pitrArchiveCheckBackoff is the wait before each of the five retries of a
// transient store error.
var pitrArchiveCheckBackoff = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}

// Exit codes of pitr-archive-check. Only exit 0 carries a verdict line.
const (
	pitrCheckExitOK        = 0
	pitrCheckExitConfig    = 2
	pitrCheckExitTransient = 3
)

// runPITRArchiveCheck is `bloodraven pitr-archive-check` (#203, IMPL-203
// §4.4), the single container of a PITR archive check Job. It probes one
// archive destination with the mounted candidate key and writes the
// controller.PITRArchiveCheckLinePrefix line to the termination log, then
// exits 0; a transient store error exits non-zero without a line.
//
// Environment:
//
//	BLOODRAVEN_PITR_DESTINATION      the normalized destination (required)
//	BLOODRAVEN_PITR_MANIFEST_PREFIX  its key prefix (must agree)
//	BLOODRAVEN_PITR_ENCRYPTION_MODE  None or Passphrase (the candidate's)
//	BLOODRAVEN_PITR_ALGORITHM        AES-256-GCM for Passphrase, empty for None
//	BLOODRAVEN_PITR_PASSPHRASE_FILE  the candidate key snapshot (Passphrase)
//	BLOODRAVEN_PITR_CHECK_ROLE       Writer or Reader
//	BLOODRAVEN_FAILOVER_GROUP_UID    the group's UID (Writer)
//	plus the S3/PVC storage env pitr-download reads.
func runPITRArchiveCheck(_ []string) {
	logger := util.NewJSONLogger(os.Stdout, slog.LevelInfo)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		cancel()
	}()
	os.Exit(pitrArchiveCheck(ctx, os.Getenv, newArchiveStore, os.Stdout, logger))
}

// pitrArchiveCheckRequest is what one check probes.
type pitrArchiveCheckRequest struct {
	role   string
	prefix string
	expect pitrarchive.Expectation
}

// pitrArchiveCheckRequestFromEnv reads and validates the check's
// expectation: a writer expects its own UID (and a derived archive ID), a
// reader no UID.
func pitrArchiveCheckRequestFromEnv(getenv func(string) string) (pitrArchiveCheckRequest, error) {
	dest, err := pitrarchive.ParseDestination(getenv("BLOODRAVEN_PITR_DESTINATION"))
	if err != nil {
		return pitrArchiveCheckRequest{}, fmt.Errorf("BLOODRAVEN_PITR_DESTINATION: %w", err)
	}
	prefix := getenv("BLOODRAVEN_PITR_MANIFEST_PREFIX")
	if prefix != dest.KeyPrefix {
		return pitrArchiveCheckRequest{}, fmt.Errorf("BLOODRAVEN_PITR_MANIFEST_PREFIX %q is not the key prefix %q of %s", prefix, dest.KeyPrefix, dest)
	}
	enc := pitrarchive.EncryptionIdentity{
		Mode:      pitrarchive.EncryptionMode(getenv("BLOODRAVEN_PITR_ENCRYPTION_MODE")),
		Algorithm: getenv("BLOODRAVEN_PITR_ALGORITHM"),
	}
	if err := enc.Validate(); err != nil {
		return pitrArchiveCheckRequest{}, fmt.Errorf("BLOODRAVEN_PITR_ENCRYPTION_MODE/ALGORITHM: %w", err)
	}
	if hasKey := getenv("BLOODRAVEN_PITR_PASSPHRASE_FILE") != ""; hasKey != (enc.Mode == pitrarchive.EncryptionPassphrase) {
		return pitrArchiveCheckRequest{}, fmt.Errorf("BLOODRAVEN_PITR_ENCRYPTION_MODE=%s with passphrase file set=%t", enc.Mode, hasKey)
	}
	req := pitrArchiveCheckRequest{role: getenv(controller.PITRArchiveCheckRoleEnv), prefix: prefix,
		expect: pitrarchive.Expectation{Destination: dest, Encryption: enc}}
	switch req.role {
	case controller.PITRArchiveCheckRoleWriter:
		uid := getenv("BLOODRAVEN_FAILOVER_GROUP_UID")
		if uid == "" {
			return pitrArchiveCheckRequest{}, errors.New("BLOODRAVEN_FAILOVER_GROUP_UID is required for the Writer role")
		}
		req.expect.Role, req.expect.SourceUID = pitrarchive.RoleWriter, types.UID(uid)
	case controller.PITRArchiveCheckRoleReader:
		req.expect.Role = pitrarchive.RoleReader
	default:
		return pitrArchiveCheckRequest{}, fmt.Errorf("%s=%q; must be %s or %s", controller.PITRArchiveCheckRoleEnv, req.role,
			controller.PITRArchiveCheckRoleWriter, controller.PITRArchiveCheckRoleReader)
	}
	return req, nil
}

// pitrArchiveCheck is the testable core of runPITRArchiveCheck. It returns
// the exit code; only exit 0 has written the verdict line (to the
// termination log and out).
func pitrArchiveCheck(ctx context.Context, getenv func(string) string, newStore func(context.Context, *sidecar.PITRConfig) (sidecar.ArchiveStore, error), out io.Writer, logger *slog.Logger) int {
	logger = logger.With("subcommand", "pitr-archive-check")
	req, err := pitrArchiveCheckRequestFromEnv(getenv)
	if err != nil {
		logger.Error("invalid pitr-archive-check config", "error", err)
		return pitrCheckExitConfig
	}
	cfg, err := pitrDownloadConfigFromEnv()
	if err == nil {
		err = checkStoreDestination(cfg, req.expect.Destination)
	}
	if err != nil {
		logger.Error("invalid archive config", "error", err)
		return pitrCheckExitConfig
	}
	store, err := newStore(ctx, cfg)
	if err != nil {
		logger.Error("init archive store", "error", err)
		return pitrCheckExitConfig
	}
	res, err := probePITRArchive(ctx, store, req, pitrArchiveCheckBackoff, logger)
	if err != nil {
		logger.Error("pitr archive check failed without a verdict", "destination", req.expect.Destination.String(), "error", err)
		return pitrCheckExitTransient
	}
	line := controller.FormatPITRArchiveCheckLine(res)
	if err := os.WriteFile(pitrArchiveCheckTerminationLogPath, []byte(line+"\n"), 0o644); err != nil {
		logger.Warn("write termination log", "error", err)
	}
	_, _ = fmt.Fprintln(out, line)
	logger.Info(controller.PITRArchiveCheckLogMsg, "destination", req.expect.Destination.String(), "role", req.role,
		"state", res.State, "reason", string(res.Reason), "archiveID", res.ArchiveID)
	return pitrCheckExitOK
}

// probePITRArchive runs sidecar.ProbeArchive, retrying a transient store
// error after each backoff step, and converts the probe to the check
// verdict. A caller bug (an invalid expectation) is never retried.
func probePITRArchive(ctx context.Context, store sidecar.ArchiveStore, req pitrArchiveCheckRequest, backoff []time.Duration, logger *slog.Logger) (controller.PITRArchiveCheckResult, error) {
	for attempt := 0; ; attempt++ {
		probe, err := sidecar.ProbeArchive(ctx, store, req.prefix, req.expect)
		if err == nil {
			return pitrArchiveCheckResult(probe, req.expect.Destination), nil
		}
		if errors.Is(err, pitrarchive.ErrInvalidExpectation) || ctx.Err() != nil || attempt == len(backoff) {
			return controller.PITRArchiveCheckResult{}, fmt.Errorf("after %d attempts: %w", attempt+1, err)
		}
		logger.Warn("pitr archive check: transient store error; retrying", "attempt", attempt+1, "error", err)
		if err := sleepCtx(ctx, backoff[attempt]); err != nil {
			return controller.PITRArchiveCheckResult{}, err
		}
	}
}

// pitrArchiveCheckResult converts a probe to the verdict line's fields: the
// state, the refusal token, and the archive ID and source of any descriptor
// that was read (a foreign archive names its owner).
func pitrArchiveCheckResult(p sidecar.ArchiveProbe, dest pitrarchive.Destination) controller.PITRArchiveCheckResult {
	res := controller.PITRArchiveCheckResult{DestinationDigest: controller.PITRArchiveCheckDestinationDigest(dest.String())}
	switch p.State {
	case sidecar.ArchiveProbeAbsent:
		res.State = controller.PITRArchiveCheckStateAbsent
	case sidecar.ArchiveProbeVerified:
		res.State = controller.PITRArchiveCheckStateVerified
	default:
		res.State = controller.PITRArchiveCheckStateRefused
		res.Reason = p.Token
	}
	if d := p.Descriptor; d != nil {
		res.ArchiveID = d.ArchiveID
		res.Source.UID, res.Source.Namespace, res.Source.Name = d.Source.UID, d.Source.Namespace, d.Source.Name
	}
	return res
}
