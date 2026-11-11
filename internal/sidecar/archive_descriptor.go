package sidecar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	// createdBody holds the bytes this session created with PutIfAbsent
	// until a read verifies them, so "archive descriptor created" is
	// logged once even when the read-back right after the create failed.
	createdBody []byte
	// lastAttempt is when Open (also through EnsureCreated) last ran.
	lastAttempt time.Time
	// unpublished holds binlog objects this session uploaded whose
	// manifest publication then failed, by key, so a retry publishes the
	// already uploaded object instead of uploading it again (F213).
	unpublished map[string]ManifestEntry

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

// errArchiveIdentityConfig marks a writer identity that cannot create the
// descriptor: a configuration error, reported as such and never as a
// transient store failure.
var errArchiveIdentityConfig = errors.New("archive identity")

// manifestGuard is what this session last saw or wrote in a site manifest.
// Saves never drop a history-integrity marker or a promotion record the
// session knows about, nor a record's promotion set or confirmation (issue
// #269), even when a caller saves a stale copy (D-34, D-43).
type manifestGuard struct {
	marker     *HistoryIntegrity
	promotions []PromotionRecord
	// evicted holds the keys (and ObservedAt) of records the
	// MaxPromotionRecords cap dropped. They are never re-added, from a
	// stale base or the guard, so the cap's choice is stable across saves
	// instead of a dropped record coming back and evicting another.
	evicted map[string]time.Time
	// floor bounds evicted: a key observed before floor is pruned from
	// evicted, and any record observed before floor that the guard does
	// not retain is treated as evicted. Such a record, re-added, would sort
	// before every retained record and be the cap's first drop anyway; the
	// floor only makes that drop independent of the protection the cap
	// gives a base's last record. Zero until the first prune.
	floor time.Time
}

// merge returns base plus the records the guard knows that base lacks,
// capped (see mergePromotionRecords), and remembers the result. appended
// is the key of the record the caller just appended to base
// (Manifest.AppendPromotionRecord), or "": only that record may survive
// in base after an eviction named it.
func (g *manifestGuard) merge(base []PromotionRecord, appended string) []PromotionRecord {
	if g.evicted == nil {
		g.evicted = map[string]time.Time{}
	}
	out := mergePromotionRecords(g.filterEvicted(g.withKnownEvidence(base), appended), g.promotions, g.evicted)
	g.promotions = append([]PromotionRecord(nil), out...)
	g.pruneEvicted()
	return out
}

// withKnownEvidence returns base with the promotion set and the
// confirmation the guard knows for a record that base holds without them.
// Both are written once and never change, so a copy that lacks one is a
// stale copy (loaded before the confirmation was saved), not a newer one.
// base itself is not modified.
func (g *manifestGuard) withKnownEvidence(base []PromotionRecord) []PromotionRecord {
	var known map[string]PromotionRecord
	for _, r := range g.promotions {
		if r.PromotionGTIDExecuted != "" || !r.ConfirmedAt.IsZero() {
			if known == nil {
				known = map[string]PromotionRecord{}
			}
			known[promotionRecordKey(r)] = r
		}
	}
	if known == nil {
		return base
	}
	var out []PromotionRecord
	for i, r := range base {
		k, ok := known[promotionRecordKey(r)]
		if !ok {
			continue
		}
		merged := r
		if merged.PromotionGTIDExecuted == "" {
			merged.PromotionGTIDExecuted = k.PromotionGTIDExecuted
		}
		if merged.ConfirmedAt.IsZero() {
			merged.ConfirmedAt, merged.ConfirmedGTIDAdded = k.ConfirmedAt, k.ConfirmedGTIDAdded
		}
		if merged == r {
			continue
		}
		if out == nil {
			out = append([]PromotionRecord(nil), base...)
		}
		out[i] = merged
	}
	if out == nil {
		return base
	}
	return out
}

// filterEvicted drops from base the records the cap already evicted, so a
// stale manifest copy cannot resurrect them, keeping only the explicit new
// append.
func (g *manifestGuard) filterEvicted(base []PromotionRecord, appended string) []PromotionRecord {
	if len(g.evicted) == 0 && g.floor.IsZero() {
		return base
	}
	retained := make(map[string]bool, len(g.promotions))
	for _, r := range g.promotions {
		retained[promotionRecordKey(r)] = true
	}
	out := make([]PromotionRecord, 0, len(base))
	for _, r := range base {
		k := promotionRecordKey(r)
		_, evicted := g.evicted[k]
		stale := !g.floor.IsZero() && r.ObservedAt.Before(g.floor) && !retained[k]
		if k != appended && (evicted || stale) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// pruneEvicted keeps evicted bounded (F197): keys observed before the
// oldest retained record move under floor.
func (g *manifestGuard) pruneEvicted() {
	if len(g.evicted) == 0 || len(g.promotions) == 0 {
		return
	}
	oldest := g.promotions[0].ObservedAt
	for _, r := range g.promotions[1:] {
		if r.ObservedAt.Before(oldest) {
			oldest = r.ObservedAt
		}
	}
	pruned := false
	for k, at := range g.evicted {
		if at.Before(oldest) {
			delete(g.evicted, k)
			pruned = true
		}
	}
	if pruned && oldest.After(g.floor) {
		g.floor = oldest
	}
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
	case errors.Is(err, errArchiveIdentityConfig):
		// Configuration, not the store: the prefix is still empty.
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
	b, ok, err := readDescriptor(ctx, s.raw, s.prefix)
	if err != nil {
		return nil, nil, s.withDestination(err)
	}
	if !ok {
		return nil, nil, nil
	}
	d, err := pitrarchive.ParseDescriptor(b)
	if err != nil {
		return nil, b, s.withDestination(err)
	}
	return d, b, nil
}

// readDescriptor reads archive.json under prefix, bounded by
// pitrarchive.MaxDescriptorSize so a corrupt or hostile object is refused
// without buffering it (F199). ok is false when it is absent. The S3 and
// PVC backends read at most one byte past the cap; any other store (test
// fakes) is read whole and then checked.
func readDescriptor(ctx context.Context, raw ArchiveStore, prefix string) ([]byte, bool, error) {
	key := pitrarchive.DescriptorKey(prefix)
	var (
		b   []byte
		ok  bool
		err error
	)
	if lg, bounded := raw.(limitedGetter); bounded {
		b, ok, err = lg.getLimited(ctx, key, pitrarchive.MaxDescriptorSize+1)
	} else {
		b, ok, err = raw.Get(ctx, key)
	}
	if err != nil {
		return nil, false, fmt.Errorf("read archive descriptor %s: %w", key, err)
	}
	if !ok {
		return nil, false, nil
	}
	if b, err = pitrarchive.ReadDescriptorBytes(bytes.NewReader(b)); err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// Open loads and verifies the descriptor. With no descriptor it classifies
// the prefix: empty gives ErrDescriptorAbsent; v2 manifests give
// PITRArchiveDescriptorMissing; any other object gives PITRArchiveLegacy.
func (s *ArchiveSession) Open(ctx context.Context) error {
	s.lastAttempt = time.Now()
	d, b, err := s.loadDescriptor(ctx)
	if err != nil {
		return s.settle(err)
	}
	if d != nil {
		err := s.accept(d)
		if err == nil {
			s.noteCreated(d, b)
		}
		return s.settle(err)
	}
	if err := s.classifyAbsent(ctx); err != nil {
		return s.settle(err)
	}
	return s.settle(ErrDescriptorAbsent)
}

// attemptedSince reports whether Open ran at or after t.
func (s *ArchiveSession) attemptedSince(t time.Time) bool {
	return !s.lastAttempt.IsZero() && !s.lastAttempt.Before(t)
}

// noteCreated logs "archive descriptor created" when b, just verified, is
// the body this session created, and forgets the body: the event is
// logged once per create, also when the read-back right after the create
// failed and a later Open is the first to verify it (F195).
func (s *ArchiveSession) noteCreated(d *pitrarchive.Descriptor, b []byte) {
	if s.createdBody == nil {
		return
	}
	mine := bytes.Equal(b, s.createdBody)
	s.createdBody = nil
	if mine {
		// The archiver's logger already carries site; repeating it here
		// would emit a duplicate "site" key.
		s.logger.Info("archive descriptor created",
			"archiveID", d.ArchiveID, "destination", s.identity.Destination.String())
	}
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
	// Creation needs the source namespace and name, which verification
	// does not. Build the descriptor before probing the store, so a
	// missing or invalid creation identity is a configuration error, not
	// a store failure retried forever (F194).
	desc, err := pitrarchive.NewDescriptor(s.identity.Source, s.identity.Destination, s.identity.Encryption, s.passphrase, s.site, s.now())
	if err != nil {
		return s.settle(fmt.Errorf("%w: cannot create the archive descriptor (POD_NAMESPACE, FAILOVER_GROUP): %v", errArchiveIdentityConfig, err))
	}
	body, err := desc.Canonical()
	if err != nil {
		return s.settle(err)
	}
	if !s.probed {
		if err := ProbeAtomicCreate(ctx, s.raw, s.prefix); err != nil {
			return s.settle(s.withDestination(err))
		}
		s.probed = true
	}
	key := pitrarchive.DescriptorKey(s.prefix)
	err = PutIfAbsent(ctx, s.raw, key, bytes.NewReader(body), int64(len(body)))
	created := err == nil
	if created {
		s.createdBody = body
	}
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
	// An S3 SDK retry of a create whose first attempt landed gets 412 for
	// our own object. The body names this site and a µs creation time, so
	// identical bytes are this writer's create, not a sibling's.
	if bytes.Equal(gotBytes, body) {
		s.createdBody = body
	}
	if err := s.accept(got); err != nil {
		return s.settle(err)
	}
	s.noteCreated(got, gotBytes)
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
	g.merge(m.Promotions, m.appendedPromotionKey())
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
	m.Promotions = g.merge(m.Promotions, m.appendedPromotionKey())
	s.guards[m.Site] = g
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
	m.appendedPromotion = nil
	return nil
}

// noteUnpublished records that entry's object was uploaded to key but its
// manifest row was not published.
func (s *ArchiveSession) noteUnpublished(key string, entry ManifestEntry) {
	if s.unpublished == nil {
		s.unpublished = map[string]ManifestEntry{}
	}
	s.unpublished[key] = entry
}

// takeUnpublished returns and forgets the unpublished upload at key.
func (s *ArchiveSession) takeUnpublished(key string) (ManifestEntry, bool) {
	entry, ok := s.unpublished[key]
	delete(s.unpublished, key)
	return entry, ok
}

// DeleteObject removes a retired archive object after Reverify (§3.5).
func (s *ArchiveSession) DeleteObject(ctx context.Context, key string) error {
	if err := s.Reverify(ctx); err != nil {
		return fmt.Errorf("refusing to delete %s: %w", key, err)
	}
	return s.store.Delete(ctx, key)
}

func promotionRecordKey(r PromotionRecord) string {
	return r.ServerUUID + "|" + strconv.FormatUint(uint64(r.ServerID), 10) + "|" + r.GTIDExecuted + "|" + strconv.FormatInt(r.ObservedAt.UnixMicro(), 10)
}

// mergePromotionRecords returns base plus every record in extra that base
// lacks and evicted does not name, ordered by ObservedAt and capped at
// MaxPromotionRecords (newest kept); the keys it drops are added to
// evicted with their ObservedAt (nil: not tracked). The cap never drops
// base's last record, the one a writer just appended: after a backward
// clock step it sorts first, and dropping it would report a promotion
// recorded that the saved manifest lacks. The guard removes evicted
// records from base first (manifestGuard.filterEvicted), so base's last
// record is never one the cap already dropped.
func mergePromotionRecords(base, extra []PromotionRecord, evicted map[string]time.Time) []PromotionRecord {
	key := promotionRecordKey
	have := make(map[string]bool, len(base))
	for _, r := range base {
		have[key(r)] = true
	}
	var missing []PromotionRecord
	for _, r := range extra {
		k := key(r)
		if _, gone := evicted[k]; !have[k] && !gone {
			have[k] = true
			missing = append(missing, r)
		}
	}
	if len(missing) == 0 {
		return base
	}
	out := append(append([]PromotionRecord(nil), base...), missing...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].ObservedAt.Before(out[j].ObservedAt) })
	n := len(out) - MaxPromotionRecords
	if n <= 0 {
		return out
	}
	protected := ""
	if len(base) > 0 {
		protected = key(base[len(base)-1])
	}
	kept := make([]PromotionRecord, 0, MaxPromotionRecords)
	for _, r := range out {
		if k := key(r); n > 0 && k != protected {
			n--
			if evicted != nil {
				evicted[k] = r.ObservedAt
			}
			continue
		}
		kept = append(kept, r)
	}
	return kept
}

// archivePrefixContents classifies a prefix that has no descriptor.
type archivePrefixContents struct {
	v2Manifests    bool // a manifest that loads as version 2 with the mounted key
	otherManifests bool // a manifest whose content is not a readable v2 manifest
	otherObjects   bool // any other non-ignored key
}

// prefixListError is a failed listing of an archive prefix. Its message
// carries the remedy for its cause (listRemedy).
type prefixListError struct {
	prefix string
	err    error
}

func (e *prefixListError) Error() string {
	return fmt.Sprintf("list archive prefix %q: %v; %s", e.prefix, e.err, listRemedy(e.err))
}

func (e *prefixListError) Unwrap() error { return e.err }

// listRemedy names the fix for a failed listing by its cause: an S3
// access denial needs s3:ListBucket, a filesystem permission error needs
// the volume to be readable, anything else is retried (F073).
func listRemedy(err error) string {
	switch {
	case isAccessDenied(err):
		return "grant s3:ListBucket on the bucket"
	case errors.Is(err, fs.ErrPermission):
		return "make the archive volume readable by the sidecar's user"
	default:
		return "check that the archive store is reachable; it is retried"
	}
}

// classifyArchivePrefix lists prefix and loads each manifest. A manifest
// whose content cannot be a v2 manifest (undecodable, another version,
// another key, tampered) counts as otherManifests; a failure to read it
// (transport, store or cancellation) is returned, never classified, so an
// outage is not reported as legacy content (F072). A listing failure is a
// *prefixListError.
func classifyArchivePrefix(ctx context.Context, store, raw ArchiveStore, prefix string) (archivePrefixContents, error) {
	var c archivePrefixContents
	listPrefix := prefix
	if listPrefix != "" {
		listPrefix += "/"
	}
	keys, err := raw.List(ctx, listPrefix)
	if err != nil {
		if ctx.Err() != nil {
			return c, ctx.Err()
		}
		return c, &prefixListError{prefix: prefix, err: err}
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
		// loadManifest succeeds only for a version-2 manifest.
		_, err := loadManifest(ctx, store, prefix, site)
		switch {
		case err == nil:
			c.v2Manifests = true
		case ctx.Err() != nil:
			return c, ctx.Err()
		case isManifestContentError(err):
			c.otherManifests = true
		default:
			return c, fmt.Errorf("read manifest %s: %w", key, err)
		}
	}
	return c, nil
}

// isManifestContentError reports whether a loadManifest failure is about
// the stored bytes (they are not a v2 manifest this key can read) rather
// than the store failing to deliver them.
func isManifestContentError(err error) bool {
	for _, content := range []error{
		errManifestDecode, ErrManifestLegacy, ErrManifestVersionUnsupported, ErrManifestInvalid,
		ErrTamperedOrDowngrade, errObjectUndecryptable,
	} {
		if errors.Is(err, content) {
			return true
		}
	}
	return false
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
// (pitr-download, the standby reconciler's discovery scan and, with #203,
// pitr-archive-check). With no descriptor it refuses PITRArchiveNotFound
// (no manifests), PITRArchiveDescriptorMissing (version-2 manifests) or
// PITRArchiveLegacy (other manifests). A listing failure refuses
// PITRArchiveListFailed with the remedy for its cause; a failed read
// (descriptor or manifest) and cancellation are returned as plain errors,
// to be retried. store may be an encrypting store; its passphrase
// verifies the keyCheck.
func OpenArchiveForRead(ctx context.Context, store ArchiveStore, prefix string, expect pitrarchive.Expectation) (*pitrarchive.Descriptor, error) {
	if expect.Role != pitrarchive.RoleReader {
		return nil, fmt.Errorf("%w: OpenArchiveForRead needs the reader role", pitrarchive.ErrInvalidExpectation)
	}
	raw, passphrase := splitArchiveStore(store)
	b, ok, err := readDescriptor(ctx, raw, prefix)
	if err != nil {
		var ve *pitrarchive.VerifyError
		if errors.As(err, &ve) {
			ve.Destination = expect.Destination.String()
		}
		return nil, err
	}
	if ok {
		return pitrarchive.ParseAndVerifyDescriptor(b, expect, passphrase)
	}
	c, err := classifyArchivePrefix(ctx, store, raw, prefix)
	if err != nil {
		var le *prefixListError
		if errors.As(err, &le) {
			return nil, &pitrarchive.VerifyError{
				Token: pitrarchive.TokenArchiveListFailed, Destination: expect.Destination.String(),
				Detail:    fmt.Sprintf("list archive prefix %q: %v", prefix, le.err),
				Procedure: listRemedy(le.err),
			}
		}
		return nil, err
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

// ArchiveProbeState is ProbeArchive's verdict on one archive prefix.
type ArchiveProbeState string

const (
	// ArchiveProbeAbsent: the prefix is empty (writer role only). A writer
	// rendered at this destination creates the descriptor.
	ArchiveProbeAbsent ArchiveProbeState = "Absent"
	// ArchiveProbeVerified: the descriptor passed VerifyDescriptor for the
	// expectation, with the store's key.
	ArchiveProbeVerified ArchiveProbeState = "Verified"
	// ArchiveProbeRefused: a check failed; Token says which.
	ArchiveProbeRefused ArchiveProbeState = "Refused"
)

// ArchiveProbe is the outcome of ProbeArchive.
type ArchiveProbe struct {
	State ArchiveProbeState
	// Token is the refusal token when State is Refused.
	Token pitrarchive.Token
	// Err is the refusal (a *pitrarchive.VerifyError) when State is
	// Refused.
	Err error
	// Descriptor is the descriptor that was read and parsed: set when
	// Verified, and on a refusal found after parsing (ArchiveForeign,
	// ArchiveCopy, ArchiveEncryptionMismatch, ArchiveKeyMismatch,
	// ArchiveIdentityMismatch), so a caller can name the archive's source.
	Descriptor *pitrarchive.Descriptor
}

// ProbeArchive classifies prefix for a one-shot check of a candidate key
// (#203 pitr-archive-check) without writing anything. store may be an
// encrypting store; its passphrase is the candidate key. It applies the
// same rules as the consumers it stands in for:
//
//   - a descriptor is parsed and verified with VerifyDescriptor for
//     expect.Role (a writer's own UID and derived archive ID, a reader's
//     optional expected UID), exactly as OpenArchiveForWrite and
//     OpenArchiveForRead do;
//   - with no descriptor, a writer gets Absent on an empty prefix and the
//     writer refusals of ArchiveSession.Open otherwise
//     (PITRArchiveDescriptorMissing, PITRArchiveLegacy); a reader gets the
//     refusals of OpenArchiveForRead (PITRArchiveDescriptorMissing,
//     PITRArchiveLegacy, PITRArchiveNotFound).
//
// A listing refused for lack of permission (an S3 403/AccessDenied, a
// filesystem EACCES) refuses PITRArchiveListFailed with the remedy for
// its cause; no other store failure is a verdict: it is returned as an
// error for the caller to retry.
func ProbeArchive(ctx context.Context, store ArchiveStore, prefix string, expect pitrarchive.Expectation) (ArchiveProbe, error) {
	switch expect.Role {
	case pitrarchive.RoleWriter:
		if expect.SourceUID == "" || expect.Destination.IsZero() {
			return ArchiveProbe{}, fmt.Errorf("%w: a writer probe needs its own UID and destination", pitrarchive.ErrInvalidExpectation)
		}
	case pitrarchive.RoleReader:
	default:
		return ArchiveProbe{}, fmt.Errorf("%w: unknown role %q", pitrarchive.ErrInvalidExpectation, expect.Role)
	}
	if err := expect.Encryption.Validate(); err != nil {
		return ArchiveProbe{}, fmt.Errorf("%w: %v", pitrarchive.ErrInvalidExpectation, err)
	}
	dest := expect.Destination.String()
	refused := func(err error, d *pitrarchive.Descriptor) (ArchiveProbe, error) {
		var ve *pitrarchive.VerifyError
		if errors.As(err, &ve) && ve.Destination == "" {
			ve.Destination = dest
		}
		tok, _ := pitrarchive.TokenOf(err)
		return ArchiveProbe{State: ArchiveProbeRefused, Token: tok, Err: err, Descriptor: d}, nil
	}

	raw, passphrase := splitArchiveStore(store)
	b, ok, err := readDescriptor(ctx, raw, prefix)
	if err != nil {
		if isVerifyError(err) {
			return refused(err, nil)
		}
		return ArchiveProbe{}, err
	}
	if ok {
		d, err := pitrarchive.ParseDescriptor(b)
		if err != nil {
			if isVerifyError(err) {
				return refused(err, nil)
			}
			return ArchiveProbe{}, err
		}
		if err := pitrarchive.VerifyDescriptor(d, expect, passphrase); err != nil {
			if isVerifyError(err) {
				return refused(err, d)
			}
			return ArchiveProbe{}, err
		}
		return ArchiveProbe{State: ArchiveProbeVerified, Descriptor: d}, nil
	}

	c, err := probeArchivePrefix(ctx, store, raw, prefix, expect.Role == pitrarchive.RoleWriter)
	if err != nil {
		var le *prefixListError
		if errors.As(err, &le) && listPermissionDenied(le.err) {
			return refused(&pitrarchive.VerifyError{
				Token: pitrarchive.TokenArchiveListFailed, Destination: dest,
				Detail:    fmt.Sprintf("list archive prefix %q: %v", prefix, le.err),
				Procedure: listRemedy(le.err),
			}, nil)
		}
		return ArchiveProbe{}, err
	}
	if expect.Role == pitrarchive.RoleWriter {
		if err := c.writerRefusal(expect.Destination); err != nil {
			return refused(err, nil)
		}
		return ArchiveProbe{State: ArchiveProbeAbsent}, nil
	}
	switch {
	case c.v2Manifests:
		return refused(descriptorMissingError(expect.Destination), nil)
	case c.otherManifests:
		return refused(legacyPrefixError(expect.Destination), nil)
	}
	return refused(&pitrarchive.VerifyError{
		Token: pitrarchive.TokenArchiveNotFound, Destination: dest,
		Detail:    "the prefix holds no archive",
		Procedure: "check the archive profile, or wait for the first archived binlog",
	}, nil)
}

// archivePageLister is a store that lists page by page (S3), so a listing
// can stop early.
type archivePageLister interface {
	ListPages(ctx context.Context, prefix string, fn func(keys []string) error) error
}

// errStopListing ends listUntil's paging.
var errStopListing = errors.New("stop listing")

// listUntil lists prefix through raw and calls visit for each key until it
// returns true. A paging store stops fetching pages there.
func listUntil(ctx context.Context, raw ArchiveStore, prefix string, visit func(key string) bool) error {
	if pl, ok := raw.(archivePageLister); ok {
		err := pl.ListPages(ctx, prefix, func(keys []string) error {
			for _, k := range keys {
				if visit(k) {
					return errStopListing
				}
			}
			return nil
		})
		if errors.Is(err, errStopListing) {
			return nil
		}
		return err
	}
	keys, err := raw.List(ctx, prefix)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if visit(k) {
			break
		}
	}
	return nil
}

// probeArchivePrefix is classifyArchivePrefix for ProbeArchive, without
// listing the whole prefix (U52): a check Job has a short deadline, and a
// prefix with no descriptor may hold a whole history (or be a shared
// bucket path). The manifests are listed by their own key prefix
// (<prefix>/manifest-) and loaded; the rest of the prefix is listed only
// when emptiness decides the verdict (a writer, with no manifest found),
// and only up to the first object that counts. The verdicts are
// classifyArchivePrefix's: a v2 manifest wins, then any other manifest or
// object.
func probeArchivePrefix(ctx context.Context, store, raw ArchiveStore, prefix string, needEmptiness bool) (archivePrefixContents, error) {
	var c archivePrefixContents
	base := prefix
	if base != "" {
		base += "/"
	}
	listErr := func(err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &prefixListError{prefix: prefix, err: err}
	}
	var sites []string
	if err := listUntil(ctx, raw, base+"manifest-", func(key string) bool {
		if site := SiteFromManifestKey(key); site != "" && manifestKey(prefix, site) == key {
			sites = append(sites, site)
		} else if !pitrarchive.IsIgnoredArchiveKey(key) {
			c.otherObjects = true
		}
		return false
	}); err != nil {
		return c, listErr(err)
	}
	for _, site := range sites {
		_, err := loadManifest(ctx, store, prefix, site)
		switch {
		case err == nil:
			c.v2Manifests = true
		case ctx.Err() != nil:
			return c, ctx.Err()
		case isManifestContentError(err):
			c.otherManifests = true
		default:
			return c, fmt.Errorf("read manifest %s: %w", manifestKey(prefix, site), err)
		}
	}
	if !needEmptiness || !c.empty() {
		return c, nil
	}
	descriptor := pitrarchive.DescriptorKey(prefix)
	if err := listUntil(ctx, raw, base, func(key string) bool {
		if key == descriptor || pitrarchive.IsIgnoredArchiveKey(key) {
			return false
		}
		c.otherObjects = true
		return true
	}); err != nil {
		return c, listErr(err)
	}
	return c, nil
}

// listPermissionDenied reports a listing refused for lack of permission:
// retrying cannot help, so a probe reports PITRArchiveListFailed.
func listPermissionDenied(err error) bool {
	return isAccessDenied(err) || errors.Is(err, fs.ErrPermission)
}
