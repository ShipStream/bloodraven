package sidecar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aws/smithy-go"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// ErrDescriptorAbsent reports an empty prefix with no descriptor: a writer
// that may establish the archive calls EnsureCreated; others wait.
var ErrDescriptorAbsent = errors.New("sidecar: archive descriptor absent")

// ErrArchiveNotVerified is returned by session operations that need a
// verified descriptor before one is established.
var ErrArchiveNotVerified = errors.New("sidecar: archive descriptor not verified")

// ArchiveSession is a writer's view of one archive prefix (§3.5). It holds
// the verified descriptor, and every manifest save and retention delete goes
// through it: Reverify first, then the single-writer save with a read-back
// compare (F34, D-43). A session is used from one goroutine; Status may be
// called concurrently.
type ArchiveSession struct {
	store      ArchiveStore // manifests and binlogs, encrypted when configured
	raw        ArchiveStore // plaintext backend: descriptor and probe
	passphrase []byte
	prefix     string
	site       string // createdBy when this session creates the descriptor
	identity   ArchiveIdentity
	logger     *slog.Logger
	now        func() time.Time

	probed bool                    // atomic create certified in this session
	desc   *pitrarchive.Descriptor // verified descriptor; nil until verified
	guards map[string]manifestGuard

	mu         sync.Mutex
	state      pitrarchive.ObservedState
	reason     pitrarchive.RefusalReason
	verifiedID string // desc.ArchiveID while Verified, for Status
	// lastErr is the latest refusal or transient failure; nil when verified.
	lastErr error
	// transient marks lastErr as an I/O or store failure rather than a
	// refusal, so status can report the store as not ready.
	transient bool
}

// manifestGuard is what this session last saw or wrote in a site manifest.
// Saves never drop a history-integrity marker or a promotion record the
// session knows about, even when a caller saves a stale copy (D-34, D-43).
type manifestGuard struct {
	marker     *HistoryIntegrity
	promotions []PromotionRecord
}

// OpenArchiveForWrite builds a writer session for prefix and opens it: an
// existing descriptor is loaded and verified. The session is returned
// whenever the configuration is usable, also with an error: nil (verified),
// ErrDescriptorAbsent (empty prefix; call EnsureCreated to establish it), a
// *pitrarchive.VerifyError (refused), or a transient store error. store is
// the store manifests are written through; an encrypting store supplies the
// passphrase and the plaintext backend for the descriptor.
func OpenArchiveForWrite(ctx context.Context, store ArchiveStore, prefix string, identity ArchiveIdentity, site string, logger *slog.Logger) (*ArchiveSession, error) {
	s, err := newArchiveSession(store, prefix, identity, site, logger)
	if err != nil {
		return nil, err
	}
	return s, s.Open(ctx)
}

func newArchiveSession(store ArchiveStore, prefix string, identity ArchiveIdentity, site string, logger *slog.Logger) (*ArchiveSession, error) {
	if err := identity.validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", pitrarchive.ErrInvalidExpectation, err)
	}
	raw, passphrase := splitArchiveStore(store)
	if encrypted := identity.Encryption.Mode == pitrarchive.EncryptionPassphrase; encrypted != (passphrase != nil) {
		return nil, fmt.Errorf("%w: encryption mode %s does not match the store (encrypting=%t)",
			pitrarchive.ErrInvalidExpectation, identity.Encryption.Mode, passphrase != nil)
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &ArchiveSession{
		store: store, raw: raw, passphrase: passphrase, prefix: prefix, site: site,
		identity: identity, logger: logger, now: time.Now,
		guards: make(map[string]manifestGuard),
		state:  pitrarchive.StateAwaitingDescriptor,
	}, nil
}

func (s *ArchiveSession) expectation() pitrarchive.Expectation {
	return pitrarchive.Expectation{
		Role:        pitrarchive.RoleWriter,
		SourceUID:   s.identity.Source.UID,
		Destination: s.identity.Destination,
		Encryption:  s.identity.Encryption,
	}
}

// Verified reports whether the session holds a verified descriptor.
func (s *ArchiveSession) Verified() bool { return s.desc != nil }

// ArchiveID is the verified descriptor's archive ID, or "".
func (s *ArchiveSession) ArchiveID() string {
	if s.desc == nil {
		return ""
	}
	return s.desc.ArchiveID
}

// Prefix is the store-relative key prefix the session writes under.
func (s *ArchiveSession) Prefix() string { return s.prefix }

// Store is the store manifests and binlogs are read and written through.
func (s *ArchiveSession) Store() ArchiveStore { return s.store }

// Err is the latest refusal or transient failure, nil when verified.
func (s *ArchiveSession) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

// Status is the archive identity block of /archiver/status (§3.7).
func (s *ArchiveSession) Status() pitrarchive.ArchiveStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := pitrarchive.ArchiveStatus{
		Destination:    s.identity.Destination.String(),
		EncryptionMode: s.identity.Encryption.Mode,
		State:          s.state,
		Reason:         s.reason,
		KeyRevision:    s.identity.KeyRevision,
	}
	if s.state == pitrarchive.StateVerified {
		st.ArchiveID = s.verifiedID
	}
	return st
}

// transientFailure reports whether the latest failure was a store error
// rather than a refusal or a missing descriptor.
func (s *ArchiveSession) transientFailure() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transient
}

// settle records the outcome of an open, create or reverify.
func (s *ArchiveSession) settle(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastErr, s.transient = err, false
	switch {
	case err == nil:
		s.state, s.reason = pitrarchive.StateVerified, ""
		if s.desc != nil {
			s.verifiedID = s.desc.ArchiveID
		}
	case errors.Is(err, ErrDescriptorAbsent):
		s.desc = nil
		s.state, s.reason = pitrarchive.StateAwaitingDescriptor, ""
	default:
		if token, ok := pitrarchive.TokenOf(err); ok {
			s.desc = nil
			s.state = pitrarchive.StateRefused
			s.reason, _ = token.Reason()
			return err
		}
		// A store failure leaves the last verified state in place; the
		// caller refuses the operation that needed it.
		s.transient = true
	}
	return err
}

func (s *ArchiveSession) loadDescriptor(ctx context.Context) (*pitrarchive.Descriptor, []byte, error) {
	key := pitrarchive.DescriptorKey(s.prefix)
	b, ok, err := s.raw.Get(ctx, key)
	if err != nil {
		return nil, nil, fmt.Errorf("read archive descriptor %s: %w", key, err)
	}
	if !ok {
		return nil, nil, nil
	}
	d, err := pitrarchive.ParseDescriptor(b)
	if err != nil {
		var ve *pitrarchive.VerifyError
		if errors.As(err, &ve) {
			ve.Destination = s.identity.Destination.String()
		}
		return nil, b, err
	}
	return d, b, nil
}

// Open loads and verifies the descriptor. With no descriptor it classifies
// the prefix: empty gives ErrDescriptorAbsent; v2 manifests give
// PITRArchiveDescriptorMissing; any other object gives PITRArchiveLegacy.
func (s *ArchiveSession) Open(ctx context.Context) error {
	d, _, err := s.loadDescriptor(ctx)
	if err != nil {
		return s.settle(err)
	}
	if d != nil {
		return s.settle(s.accept(d))
	}
	if err := s.classifyAbsent(ctx); err != nil {
		return s.settle(err)
	}
	return s.settle(ErrDescriptorAbsent)
}

func (s *ArchiveSession) accept(d *pitrarchive.Descriptor) error {
	if err := pitrarchive.VerifyDescriptor(d, s.expectation(), s.passphrase); err != nil {
		return err
	}
	s.desc = d
	return nil
}

func (s *ArchiveSession) classifyAbsent(ctx context.Context) error {
	c, err := classifyArchivePrefix(ctx, s.store, s.raw, s.prefix)
	if err != nil {
		return err
	}
	return c.writerRefusal(s.identity.Destination)
}

// EnsureCreated establishes the archive (§3.5): it opens the prefix and,
// only when it is empty, certifies atomic create once per session, creates
// the descriptor with PutIfAbsent and reads it back. A sibling that won the
// race is verified like any existing descriptor, so a foreign one refuses.
// There is no Get-then-Put fallback.
func (s *ArchiveSession) EnsureCreated(ctx context.Context) error {
	err := s.Open(ctx)
	if !errors.Is(err, ErrDescriptorAbsent) {
		return err
	}
	if !s.probed {
		if err := ProbeAtomicCreate(ctx, s.raw, s.prefix); err != nil {
			return s.settle(s.withDestination(err))
		}
		s.probed = true
	}
	desc, err := pitrarchive.NewDescriptor(s.identity.Source, s.identity.Destination, s.identity.Encryption, s.passphrase, s.site, s.now())
	if err != nil {
		return s.settle(fmt.Errorf("%w: %v", pitrarchive.ErrInvalidExpectation, err))
	}
	body, err := desc.Canonical()
	if err != nil {
		return s.settle(err)
	}
	key := pitrarchive.DescriptorKey(s.prefix)
	err = PutIfAbsent(ctx, s.raw, key, bytes.NewReader(body), int64(len(body)))
	created := err == nil
	switch {
	case created, errors.Is(err, ErrObjectExists):
	case errors.Is(err, ErrConditionalWriteUnsupported):
		return s.settle(&pitrarchive.VerifyError{
			Token: pitrarchive.TokenArchiveStoreUnsupported, Destination: s.identity.Destination.String(),
			Detail: err.Error(), Procedure: "use a store that enforces conditional writes",
		})
	default:
		return s.settle(fmt.Errorf("create archive descriptor %s: %w", key, err))
	}
	got, gotBytes, err := s.loadDescriptor(ctx)
	if err != nil {
		return s.settle(err)
	}
	if got == nil {
		return s.settle(fmt.Errorf("archive descriptor %s is not readable after create", key))
	}
	if created && !bytes.Equal(gotBytes, body) {
		return s.settle(fmt.Errorf("archive descriptor %s read back different bytes than were created", key))
	}
	if err := s.accept(got); err != nil {
		return s.settle(err)
	}
	if created {
		s.logger.Info("archive descriptor created",
			"archiveID", got.ArchiveID, "destination", s.identity.Destination.String(), "site", s.site)
	}
	return s.settle(nil)
}

func (s *ArchiveSession) withDestination(err error) error {
	var ve *pitrarchive.VerifyError
	if errors.As(err, &ve) && ve.Destination == "" {
		ve.Destination = s.identity.Destination.String()
	}
	return err
}

// Reverify re-reads the descriptor and requires it to be identical, in
// identity, to the verified one. It runs before every manifest save and
// every retention delete (§3.5). A changed or vanished descriptor refuses
// and leaves the session Refused until a later Open verifies again.
func (s *ArchiveSession) Reverify(ctx context.Context) error {
	if s.desc == nil {
		if err := s.Err(); err != nil {
			return fmt.Errorf("%w: %w", ErrArchiveNotVerified, err)
		}
		return ErrArchiveNotVerified
	}
	verified := s.desc
	d, _, err := s.loadDescriptor(ctx)
	if err != nil && !isVerifyError(err) {
		s.mu.Lock()
		s.lastErr, s.transient = err, true
		s.mu.Unlock()
		return err
	}
	if err == nil {
		switch {
		case d == nil:
			err = &pitrarchive.VerifyError{
				Token: pitrarchive.TokenArchiveDescriptorMissing, ArchiveID: verified.ArchiveID,
				Destination: s.identity.Destination.String(),
				Detail:      "archive.json disappeared while this writer held it",
				Procedure:   "restore the descriptor from a copy of this archive, or start a new archive prefix and take a full backup",
			}
		default:
			if err = pitrarchive.VerifyDescriptor(d, s.expectation(), s.passphrase); err == nil && !pitrarchive.IdentityEqual(d, verified) {
				err = &pitrarchive.VerifyError{
					Token: pitrarchive.TokenArchiveCopy, ArchiveID: d.ArchiveID,
					Destination: s.identity.Destination.String(),
					Detail:      fmt.Sprintf("archive.json changed identity while this writer held archive %s", verified.ArchiveID),
					Procedure:   "point this profile at a new prefix",
				}
			}
		}
	}
	if err != nil {
		s.logger.Warn("archive descriptor changed",
			"archiveID", verified.ArchiveID, "destination", s.identity.Destination.String(), "error", err)
		return s.settle(err)
	}
	s.desc = d
	return s.settle(nil)
}

func isVerifyError(err error) bool {
	_, ok := pitrarchive.TokenOf(err)
	return ok
}

// LoadManifest reads site's manifest. A manifest written for another
// archive ID refuses PITRArchiveObjectInvalid. An absent manifest loads as
// an empty v2 manifest; SaveManifest stamps its archive ID.
func (s *ArchiveSession) LoadManifest(ctx context.Context, site string) (*Manifest, error) {
	if s.desc == nil {
		return nil, s.notVerified()
	}
	m, err := loadManifest(ctx, s.store, s.prefix, site)
	if err != nil {
		return nil, err
	}
	if err := s.checkManifestArchive(m); err != nil {
		return nil, err
	}
	s.remember(m)
	return m, nil
}

func (s *ArchiveSession) notVerified() error {
	if err := s.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrArchiveNotVerified, err)
	}
	return ErrArchiveNotVerified
}

func (s *ArchiveSession) checkManifestArchive(m *Manifest) error {
	if m.ArchiveID != "" && m.ArchiveID != s.desc.ArchiveID {
		return fmt.Errorf("%w: manifest %s belongs to archive %s, but %s holds archive %s; start a new archive prefix and take a full backup",
			ErrManifestInvalid, manifestKey(s.prefix, m.Site), m.ArchiveID, pitrarchive.DescriptorKey(s.prefix), s.desc.ArchiveID)
	}
	return nil
}

func (s *ArchiveSession) remember(m *Manifest) {
	g := s.guards[m.Site]
	if m.HistoryIntegrity != nil && g.marker == nil {
		marker := *m.HistoryIntegrity
		g.marker = &marker
	}
	g.promotions = mergePromotionRecords(g.promotions, m.Promotions)
	s.guards[m.Site] = g
}

// SaveManifest is the only manifest write path for writers: Reverify, stamp
// the archive ID, keep every marker and promotion record this session knows,
// Put, then read back and compare the bytes (F34, D-43).
func (s *ArchiveSession) SaveManifest(ctx context.Context, m *Manifest) error {
	if err := s.Reverify(ctx); err != nil {
		return fmt.Errorf("refusing to save manifest: %w", err)
	}
	if m.Site == "" || !isPlainFileName(m.Site) {
		return fmt.Errorf("refusing to save manifest: invalid site %q", m.Site)
	}
	if m.ArchiveID == "" {
		m.ArchiveID = s.desc.ArchiveID
	}
	if err := s.checkManifestArchive(m); err != nil {
		return fmt.Errorf("refusing to save: %w", err)
	}
	g := s.guards[m.Site]
	if m.HistoryIntegrity == nil && g.marker != nil {
		marker := *g.marker
		m.HistoryIntegrity = &marker
	}
	m.Promotions = mergePromotionRecords(m.Promotions, g.promotions)
	buf, err := encodeManifest(s.prefix, m)
	if err != nil {
		return err
	}
	key := manifestKey(s.prefix, m.Site)
	if err := s.store.Put(ctx, key, bytes.NewReader(buf), int64(len(buf))); err != nil {
		return err
	}
	back, ok, err := s.store.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("read back manifest %s: %w", key, err)
	}
	if !ok || !bytes.Equal(back, buf) {
		return fmt.Errorf("read back manifest %s: stored bytes differ from the save; another writer may own this site", key)
	}
	s.remember(m)
	return nil
}

// DeleteObject removes a retired archive object after Reverify (§3.5).
func (s *ArchiveSession) DeleteObject(ctx context.Context, key string) error {
	if err := s.Reverify(ctx); err != nil {
		return fmt.Errorf("refusing to delete %s: %w", key, err)
	}
	return s.store.Delete(ctx, key)
}

// mergePromotionRecords returns base plus every record in extra that base
// lacks, ordered by ObservedAt and capped at MaxPromotionRecords (newest kept).
func mergePromotionRecords(base, extra []PromotionRecord) []PromotionRecord {
	key := func(r PromotionRecord) string {
		return r.ServerUUID + "|" + strconv.FormatUint(uint64(r.ServerID), 10) + "|" + r.GTIDExecuted + "|" + strconv.FormatInt(r.ObservedAt.UnixMicro(), 10)
	}
	have := make(map[string]bool, len(base))
	for _, r := range base {
		have[key(r)] = true
	}
	var missing []PromotionRecord
	for _, r := range extra {
		if !have[key(r)] {
			have[key(r)] = true
			missing = append(missing, r)
		}
	}
	if len(missing) == 0 {
		return base
	}
	out := append(append([]PromotionRecord(nil), base...), missing...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].ObservedAt.Before(out[j].ObservedAt) })
	if n := len(out) - MaxPromotionRecords; n > 0 {
		out = out[n:]
	}
	return out
}

// archivePrefixContents classifies a prefix that has no descriptor.
type archivePrefixContents struct {
	v2Manifests    bool // a manifest that loads as version 2 with the mounted key
	otherManifests bool // a manifest that does not
	otherObjects   bool // any other non-ignored key
}

func classifyArchivePrefix(ctx context.Context, store, raw ArchiveStore, prefix string) (archivePrefixContents, error) {
	var c archivePrefixContents
	listPrefix := prefix
	if listPrefix != "" {
		listPrefix += "/"
	}
	keys, err := raw.List(ctx, listPrefix)
	if err != nil {
		if isAccessDenied(err) {
			return c, fmt.Errorf("list archive prefix %q: %w; grant s3:ListBucket on the bucket", prefix, err)
		}
		return c, fmt.Errorf("list archive prefix %q: %w", prefix, err)
	}
	descriptor := pitrarchive.DescriptorKey(prefix)
	for _, key := range keys {
		if key == descriptor || pitrarchive.IsIgnoredArchiveKey(key) {
			continue
		}
		site := SiteFromManifestKey(key)
		if site == "" || manifestKey(prefix, site) != key {
			c.otherObjects = true
			continue
		}
		if m, err := loadManifest(ctx, store, prefix, site); err == nil && m.Version == ManifestVersion {
			c.v2Manifests = true
		} else {
			if ctx.Err() != nil {
				return c, ctx.Err()
			}
			c.otherManifests = true
		}
	}
	return c, nil
}

func (c archivePrefixContents) empty() bool {
	return !c.v2Manifests && !c.otherManifests && !c.otherObjects
}

func (c archivePrefixContents) writerRefusal(dest pitrarchive.Destination) error {
	switch {
	case c.v2Manifests:
		return descriptorMissingError(dest)
	case !c.empty():
		return legacyPrefixError(dest)
	}
	return nil
}

func descriptorMissingError(dest pitrarchive.Destination) error {
	return &pitrarchive.VerifyError{
		Token: pitrarchive.TokenArchiveDescriptorMissing, Destination: dest.String(),
		Detail:    "the prefix holds version-2 manifests but no archive.json",
		Procedure: "restore the descriptor, or wait for replication to deliver it",
	}
}

func legacyPrefixError(dest pitrarchive.Destination) error {
	return &pitrarchive.VerifyError{
		Token: pitrarchive.TokenArchiveLegacy, Destination: dest.String(),
		Detail:    "the prefix holds objects written before archive identity existed",
		Procedure: "start a new archive prefix and take a full backup",
	}
}

func isAccessDenied(err error) bool {
	var withStatus interface{ HTTPStatusCode() int }
	if errors.As(err, &withStatus) && withStatus.HTTPStatusCode() == http.StatusForbidden {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "AccessDenied"
}

// OpenArchiveForRead loads and verifies the descriptor for a reader
// (pitr-download, dump-meta, pitr-archive-check). With no descriptor it
// refuses PITRArchiveNotFound (no manifests), PITRArchiveDescriptorMissing
// (version-2 manifests) or PITRArchiveLegacy (other manifests). A listing
// failure refuses PITRArchiveListFailed. store may be an encrypting store;
// its passphrase verifies the keyCheck.
func OpenArchiveForRead(ctx context.Context, store ArchiveStore, prefix string, expect pitrarchive.Expectation) (*pitrarchive.Descriptor, error) {
	if expect.Role != pitrarchive.RoleReader {
		return nil, fmt.Errorf("%w: OpenArchiveForRead needs the reader role", pitrarchive.ErrInvalidExpectation)
	}
	raw, passphrase := splitArchiveStore(store)
	key := pitrarchive.DescriptorKey(prefix)
	b, ok, err := raw.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("read archive descriptor %s: %w", key, err)
	}
	if ok {
		return pitrarchive.ParseAndVerifyDescriptor(b, expect, passphrase)
	}
	c, err := classifyArchivePrefix(ctx, store, raw, prefix)
	if err != nil {
		return nil, &pitrarchive.VerifyError{
			Token: pitrarchive.TokenArchiveListFailed, Destination: expect.Destination.String(),
			Detail: err.Error(), Procedure: "grant s3:ListBucket on the bucket",
		}
	}
	switch {
	case c.v2Manifests:
		return nil, descriptorMissingError(expect.Destination)
	case c.otherManifests:
		return nil, legacyPrefixError(expect.Destination)
	}
	return nil, &pitrarchive.VerifyError{
		Token: pitrarchive.TokenArchiveNotFound, Destination: expect.Destination.String(),
		Detail:    "the prefix holds no archive",
		Procedure: "check the archive profile, or wait for the first archived binlog",
	}
}
