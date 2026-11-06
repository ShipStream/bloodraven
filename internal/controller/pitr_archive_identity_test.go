package controller

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// identityFG is fgWithBackup with UIDs and PITR into its S3 profile.
func identityFG() *v1alpha1.MysqlFailoverGroup {
	fg := fgWithBackup()
	fg.UID = "fg-uid"
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	return fg
}

func identityBackup() *v1alpha1.MysqlBackup {
	return &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-nightly-1", Namespace: "ns", UID: "backup-uid"},
		Spec:       v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"}, ProfileName: "nightly-s3"},
	}
}

func identityArchiveID(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) string {
	t.Helper()
	ident, ok := activePITRArchiveIdentity(fg)
	if !ok {
		t.Fatal("no active archive")
	}
	return ident.ArchiveID
}

func stampTemplateFromContainer(t *testing.T, c corev1.Container) backupmeta.Stamp {
	t.Helper()
	raw := envMap(c.Env)["BLOODRAVEN_BACKUP_STAMP"]
	if raw == "" {
		t.Fatalf("container %s has no BLOODRAVEN_BACKUP_STAMP", c.Name)
	}
	var s backupmeta.Stamp
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatal(err)
	}
	return s
}

// D-8: every backup Job starts with the backup-stamp init container, pins the
// stamp identity (backup, source, archive, site, server_id, filters) and the
// Job annotations, and runs mysqlsh with TZ=UTC. Plaintext Jobs install the
// stamp tool for dump.py; encrypted Jobs finish the stamp in the uploader and
// mount the passphrase on the stamp container, never on mysqlsh.
func TestBuildBackupJob_BackupStamp(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	for _, encrypted := range []bool{false, true} {
		t.Run(map[bool]string{false: "plaintext", true: "encrypted"}[encrypted], func(t *testing.T) {
			fg := identityFG()
			profile := fg.Spec.Backup.Profiles[0]
			profile.Dump.IncludeSchemas = []string{"orders"}
			if encrypted {
				profile.Encryption = &v1alpha1.BackupEncryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "pp"}}
			}
			job, err := BuildBackupJob(BackupJobInputs{
				FailoverGroup: fg, Profile: profile, Backup: identityBackup(), SourceSite: "pdx",
				CredsSecretName: "creds", ScriptsConfigMapName: "scripts",
			})
			if err != nil {
				t.Fatal(err)
			}
			spec := job.Spec.Template.Spec
			stampC := spec.InitContainers[0]
			if stampC.Name != backupStampInitContainerName || stampC.Image != "bloodraven:test" ||
				strings.Join(stampC.Command, " ") != operatorBinaryPath+" backup-stamp" {
				t.Fatalf("first init container: %s %s %v", stampC.Name, stampC.Image, stampC.Command)
			}
			stamp := stampTemplateFromContainer(t, stampC)
			want := backupmeta.Stamp{
				Kind: backupmeta.StampKind, Version: backupmeta.StampVersion,
				Backup:        backupmeta.ObjectRef{Namespace: "ns", Name: "lion-nightly-1", UID: "backup-uid"},
				Source:        backupmeta.ObjectRef{Namespace: "ns", Name: "lion", UID: "fg-uid"},
				PITRArchiveID: identityArchiveID(t, fg),
				Site:          "pdx", ServerID: 102,
				DumpFilters: backupmeta.DumpFilters{IncludeSchemas: []string{"orders"}},
			}
			if a, b := mustCanonical(t, stamp), mustCanonical(t, want); a != b {
				t.Fatalf("stamp template\n got %s\nwant %s", a, b)
			}
			if !stamp.ServerClock.Start.IsZero() || !stamp.CreatedAt.IsZero() {
				t.Fatal("the Job, not the operator, sets serverClock and createdAt")
			}
			for _, obj := range []map[string]string{job.Annotations, job.Spec.Template.Annotations} {
				if obj[BackupJobFailoverGroupUIDAnnotation] != "fg-uid" || obj[BackupJobPITRArchiveIDAnnotation] != want.PITRArchiveID {
					t.Fatalf("identity annotations: %v", obj)
				}
			}
			stampEnv := envMap(stampC.Env)
			if stampEnv["BLOODRAVEN_MYSQL_HOST"] != backupMySQLHost(fg, "pdx") || stampEnv["TZ"] != "UTC" {
				t.Fatalf("stamp env: %v", stampEnv)
			}
			var dump, finisher corev1.Container
			if encrypted {
				dump, finisher = spec.InitContainers[1], spec.Containers[0]
				if envMap(stampC.Env)["BLOODRAVEN_BACKUP_PASSPHRASE_FILE"] == "" || findMount(stampC.VolumeMounts, backupPassphraseMountPath) == nil {
					t.Fatal("the stamp is BRV1-encrypted with the dump key: the stamp container needs the passphrase")
				}
				if findMount(dump.VolumeMounts, backupPassphraseMountPath) != nil {
					t.Fatal("mysqlsh must never mount the passphrase")
				}
			} else {
				dump, finisher = spec.Containers[0], spec.Containers[0]
				if envMap(dump.Env)["BLOODRAVEN_STAMP_TOOL"] != backupStampToolMountPath+"/bloodraven" ||
					envMap(stampC.Env)["BLOODRAVEN_STAMP_TOOL_DIR"] != backupStampToolMountPath ||
					findMount(dump.VolumeMounts, backupStampToolMountPath) == nil {
					t.Fatal("plaintext Jobs install the stamp tool for dump.py's end step")
				}
			}
			if envMap(dump.Env)["TZ"] != "UTC" {
				t.Fatalf("mysqlsh needs TZ=UTC for @.done.json: %v", envMap(dump.Env))
			}
			if a, b := mustCanonical(t, stampTemplateFromContainer(t, finisher)), mustCanonical(t, want); a != b {
				t.Fatalf("the end step pins another stamp: %s", a)
			}
		})
	}
}

func mustCanonical(t *testing.T, s backupmeta.Stamp) string {
	t.Helper()
	b, err := s.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// A backup of a group without PITR has no archive ID; a source site outside
// spec.sites cannot be stamped.
func TestBackupStampTemplate(t *testing.T) {
	fg := identityFG()
	fg.Spec.Backup.PITR = nil
	s, err := backupStampTemplate(fg, identityBackup(), fg.Spec.Backup.Profiles[0], "iad")
	if err != nil || s.PITRArchiveID != "" || s.ServerID != 101 {
		t.Fatalf("stamp %+v, err %v", s, err)
	}
	if _, err := backupStampTemplate(fg, identityBackup(), fg.Spec.Backup.Profiles[0], "ghost"); err == nil {
		t.Fatal("unknown source site accepted")
	}
}

// F21: the Job is created before the status patch. A lost patch leaves the
// identity on the Job only; the next reconcile recovers it, and never trusts
// a Job it does not own.
func TestBackupReconciler_RecoversIdentityFromJob(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	for _, owned := range []bool{true, false} {
		t.Run(map[bool]string{true: "owned", false: "foreign"}[owned], func(t *testing.T) {
			fg := identityFG()
			backup := identityBackup()
			r, c := newBackupReconciler(t, fg, backup)
			job, err := BuildBackupJob(BackupJobInputs{
				FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Backup: backup, SourceSite: "pdx",
				CredsSecretName: "creds", ScriptsConfigMapName: "scripts",
			})
			if err != nil {
				t.Fatal(err)
			}
			if owned {
				job.OwnerReferences = []metav1.OwnerReference{{APIVersion: "shipstream.io/v1alpha1", Kind: "MysqlBackup", Name: backup.Name, UID: backup.UID, Controller: boolPtr(true)}}
			}
			if err := c.Create(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			// The first passes add the finalizer and the owner reference.
			for range 4 {
				if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: backup.Name}}); err != nil {
					t.Fatal(err)
				}
			}
			var got v1alpha1.MysqlBackup
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(backup), &got); err != nil {
				t.Fatal(err)
			}
			wantUID, wantID := types.UID("fg-uid"), identityArchiveID(t, fg)
			if !owned {
				wantUID, wantID = "", ""
			}
			if got.Status.FailoverGroupUID != wantUID || got.Status.PITRArchiveID != wantID {
				t.Fatalf("status identity = %q/%q, want %q/%q", got.Status.FailoverGroupUID, got.Status.PITRArchiveID, wantUID, wantID)
			}
		})
	}
}

func TestBackupJobIdentity(t *testing.T) {
	if uid, id := backupJobIdentity(nil); uid != "" || id != "" {
		t.Fatal("nil Job has no identity")
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		BackupJobFailoverGroupUIDAnnotation: "u", BackupJobPITRArchiveIDAnnotation: "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa",
	}}}
	if uid, id := backupJobIdentity(job); uid != "u" || id != "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("got %q %q", uid, id)
	}
}

func archiveStatus(dest string, state pitrarchive.ObservedState, id string, reason pitrarchive.RefusalReason) *internalmysql.ArchiverStatus {
	return &internalmysql.ArchiverStatus{Enabled: true, StoreReady: pitrBool(state != pitrarchive.StateRefused),
		Archive: &pitrarchive.ArchiveStatus{Destination: dest, ArchiveID: id, EncryptionMode: pitrarchive.EncryptionNone, State: state, Reason: reason}}
}

// status.pitr.archive counts only sidecars that report the active
// destination; any refusal wins, Verified needs every site.
func TestPITRArchiveObservation(t *testing.T) {
	fg := identityFG()
	ident, _ := activePITRArchiveIdentity(fg)
	dest, id := ident.Destination.String(), ident.ArchiveID
	sites := []string{"iad", "pdx"}
	tests := []struct {
		name       string
		snaps      map[string]*internalmysql.ArchiverStatus
		wantState  v1alpha1.PITRArchiveObservedState
		wantReason v1alpha1.PITRArchiveRefusalReason
		wantID     string
	}{
		{name: "no reports", snaps: map[string]*internalmysql.ArchiverStatus{}, wantState: v1alpha1.PITRArchiveAwaitingDescriptor},
		{name: "all verified", snaps: map[string]*internalmysql.ArchiverStatus{"iad": archiveStatus(dest, pitrarchive.StateVerified, id, ""), "pdx": archiveStatus(dest, pitrarchive.StateVerified, id, "")}, wantState: v1alpha1.PITRArchiveVerified, wantID: id},
		{name: "one site still awaiting", snaps: map[string]*internalmysql.ArchiverStatus{"iad": archiveStatus(dest, pitrarchive.StateVerified, id, ""), "pdx": archiveStatus(dest, pitrarchive.StateAwaitingDescriptor, "", "")}, wantState: v1alpha1.PITRArchiveAwaitingDescriptor, wantID: id},
		{name: "old render is not counted", snaps: map[string]*internalmysql.ArchiverStatus{"iad": archiveStatus(dest, pitrarchive.StateVerified, id, ""), "pdx": archiveStatus("s3://old/binlogs?endpoint=aws", pitrarchive.StateRefused, "", pitrarchive.ReasonArchiveLegacy)}, wantState: v1alpha1.PITRArchiveAwaitingDescriptor, wantID: id},
		{name: "refusal wins, most specific first", snaps: map[string]*internalmysql.ArchiverStatus{"iad": archiveStatus(dest, pitrarchive.StateRefused, "", pitrarchive.ReasonArchiveLegacy), "pdx": archiveStatus(dest, pitrarchive.StateRefused, "", pitrarchive.ReasonArchiveForeign)}, wantState: v1alpha1.PITRArchiveRefused, wantReason: "ArchiveForeign"},
		{name: "history marker refuses a verified archive", snaps: map[string]*internalmysql.ArchiverStatus{"iad": func() *internalmysql.ArchiverStatus {
			s := archiveStatus(dest, pitrarchive.StateVerified, id, "")
			s.HistoryIntegrity = &pitrarchive.HistoryIntegrity{Violation: pitrarchive.ViolationBinlogFormat}
			return s
		}(), "pdx": archiveStatus(dest, pitrarchive.StateVerified, id, "")}, wantState: v1alpha1.PITRArchiveRefused, wantReason: "ArchiveUnsafeHistory", wantID: id},
		{name: "two archive IDs at one destination", snaps: map[string]*internalmysql.ArchiverStatus{"iad": archiveStatus(dest, pitrarchive.StateVerified, id, ""), "pdx": archiveStatus(dest, pitrarchive.StateVerified, "ar1-bbbbbbbbbbbbbbbbbbbbbbbbbb", "")}, wantState: v1alpha1.PITRArchiveRefused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 11, 4, 12, 0, 0, 123456789, time.UTC)
			got := pitrArchiveObservation(fg, sites, tt.snaps, nil, nil, now)
			if got == nil || got.Destination != dest || got.State != tt.wantState || got.Reason != tt.wantReason || got.ArchiveID != tt.wantID {
				t.Fatalf("observation = %+v, want state %s reason %s id %q", got, tt.wantState, tt.wantReason, tt.wantID)
			}
			if got.ObservedTime == nil || !got.ObservedTime.Equal(&metav1.MicroTime{Time: now.Truncate(time.Microsecond)}) {
				t.Fatalf("observedTime = %v", got.ObservedTime)
			}
			// An unchanged observation keeps its time; a change moves it.
			again := pitrArchiveObservation(fg, sites, tt.snaps, nil, got, now.Add(time.Minute))
			if !again.ObservedTime.Equal(got.ObservedTime) {
				t.Fatal("unchanged observation moved observedTime")
			}
		})
	}
	off := identityFG()
	off.Spec.Backup.PITR.ProfileName = "ghost"
	if got := pitrArchiveObservation(off, sites, nil, nil, nil, time.Now()); got != nil {
		t.Fatalf("no active archive, but observation %+v", got)
	}
}

// D-27: an archive refusal is reported at once, by its own reason, ranked
// after ArchiveStoreInitFailed.
func TestPITRArchiveTrackerArchiveRefusals(t *testing.T) {
	refused := func(reason pitrarchive.RefusalReason) pitrObs {
		s := archiveStatus("d", pitrarchive.StateRefused, "", reason)
		s.LastError = "PITR" + string(reason) + ": archive at d: detail; procedure"
		return pitrObs{status: s}
	}
	marked := pitrObs{status: &internalmysql.ArchiverStatus{Enabled: true, StoreReady: pitrBool(true),
		HistoryIntegrity: &pitrarchive.HistoryIntegrity{Violation: pitrarchive.ViolationReplicationFilter, Detail: "Replicate_Do_DB=x"}}}
	tokenOnly := pitrRunError("PITRArchiveStoreUnsupported: archive at d: the store ignored If-None-Match; use a store that enforces conditional writes")
	tests := []struct {
		name     string
		iad, pdx pitrObs
		want     string
	}{
		{name: "legacy", iad: refused(pitrarchive.ReasonArchiveLegacy), pdx: pitrClean(), want: "ArchiveLegacy"},
		{name: "marker", iad: marked, pdx: pitrClean(), want: "ArchiveUnsafeHistory"},
		{name: "token in lastError", iad: tokenOnly, pdx: pitrClean(), want: "ArchiveStoreUnsupported"},
		{name: "store init outranks", iad: refused(pitrarchive.ReasonArchiveCopy), pdx: pitrInitFailed("boom"), want: reasonPITRArchiveStoreInitFailed},
		{name: "outranks an archiver error", iad: refused(pitrarchive.ReasonArchiveKeyMismatch), pdx: pitrRunError("x"), want: "ArchiveKeyMismatch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newPITRArchiveTracker(nil)
			tr.observe(time.Now(), pitrRound{iad: tt.iad, pdx: tt.pdx}.results())
			if v := tr.current(); v.Status != metav1.ConditionFalse || v.Reason != tt.want {
				t.Fatalf("verdict %+v, want False/%s", v, tt.want)
			}
		})
	}
}

// N6, F033, F145: while PITR is enabled an option that breaks replay
// certification is refused from each site's effective configuration (the
// group's mysqlConf overlaid with the site's), and the refusal names the
// site and the field it comes from.
func TestPITRRefusedMySQLConf(t *testing.T) {
	tests := []struct {
		name       string
		conf, site map[string]string
		pitr       bool
		want       []string // "site: substring", sorted
	}{
		{name: "clean", conf: map[string]string{"max_connections": "100", "binlog_format": "ROW", "binlog-row-image": "full"}, pitr: true},
		{name: "replicate filter on the group hits every site", conf: map[string]string{"replicate_do_db": "orders"}, pitr: true,
			want: []string{"iad: spec.mysqlConf sets replicate-do-db", "pdx: spec.mysqlConf sets replicate-do-db"}},
		{name: "wild filter, loose prefix", conf: map[string]string{"loose-replicate-wild-ignore-table": "a.%"}, pitr: true,
			want: []string{"iad: spec.mysqlConf sets loose-replicate-wild-ignore-table", "pdx: spec.mysqlConf sets loose-replicate-wild-ignore-table"}},
		{name: "binlog filter on one site is that site's", site: map[string]string{"binlog-ignore-db": "tmp"}, pitr: true,
			want: []string{"pdx: spec.sites[pdx].mysqlConf sets binlog-ignore-db"}},
		{name: "statement format", conf: map[string]string{"binlog_format": "MIXED"}, pitr: true,
			want: []string{"iad: spec.mysqlConf sets binlog-format=MIXED", "pdx: spec.mysqlConf sets binlog-format=MIXED"}},
		{name: "site override makes the effective value safe", conf: map[string]string{"binlog_format": "MIXED"}, site: map[string]string{"binlog-format": "row"}, pitr: true,
			want: []string{"iad: spec.mysqlConf sets binlog-format=MIXED"}},
		{name: "site sets the unsafe value", site: map[string]string{"binlog_row_image": "MINIMAL"}, pitr: true,
			want: []string{"pdx: spec.sites[pdx].mysqlConf sets binlog-row-image=MINIMAL"}},
		{name: "PITR disabled", conf: map[string]string{"replicate_do_db": "orders"}, pitr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fg := identityFG()
			fg.Spec.Backup.PITR.Enabled = tt.pitr
			fg.Spec.MysqlConf = tt.conf
			fg.Spec.Sites[1].MysqlConf = tt.site
			if fg.Spec.Sites[0].Name != "iad" || fg.Spec.Sites[1].Name != "pdx" {
				t.Fatalf("fixture sites %+v", fg.Spec.Sites)
			}
			got := pitrMySQLConfRefusalMessage(fg)
			parts := []string{}
			if got != "" {
				parts = strings.Split(got, "; ")
			}
			if len(parts) != len(tt.want) {
				t.Fatalf("refusals %q, want %q", got, tt.want)
			}
			for i, w := range tt.want {
				if !strings.HasPrefix(parts[i], w) {
					t.Fatalf("refusal %d %q, want prefix %q", i, parts[i], w)
				}
			}
			// The render drops exactly the refused options.
			for _, site := range fg.Spec.Sites {
				cnf := generateMyCnf(fg, site)
				for _, ref := range pitrRefusedMySQLConf(fg, site) {
					if strings.Contains(cnf, "\n"+ref.key+"=") || strings.Contains(cnf, "\n"+ref.key+"\n") {
						t.Fatalf("site %s renders refused %s:\n%s", site.Name, ref.key, cnf)
					}
				}
			}
		})
	}
	// Without PITR the same option is rendered as configured.
	fg := identityFG()
	fg.Spec.Backup.PITR.Enabled = false
	fg.Spec.MysqlConf = map[string]string{"binlog_format": "MIXED"}
	if cnf := generateMyCnf(fg, fg.Spec.Sites[0]); !strings.Contains(cnf, "\nbinlog-format=MIXED\n") {
		t.Fatalf("PITR disabled, but binlog-format dropped:\n%s", cnf)
	}
	fg.Spec.Backup.PITR.Enabled = true
	if cnf := generateMyCnf(fg, fg.Spec.Sites[0]); strings.Contains(cnf, "binlog-format") {
		t.Fatalf("PITR enabled, but binlog-format rendered:\n%s", cnf)
	}
}

// N6: the refusal is reported as PITRMysqlConfAccepted=False with a Warning
// PITRMysqlConfRefused once per refused set, True once fixed, and the
// condition is removed without PITR.
func TestReconcilePITRMySQLConfCondition(t *testing.T) {
	ctx := context.Background()
	fg := identityFG()
	fg.Spec.MysqlConf = map[string]string{"binlog_format": "STATEMENT"}
	r, c := newReconciler(fg)
	rec := r.Recorder.(*record.FakeRecorder)
	get := func() *metav1.Condition {
		t.Helper()
		var cur v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, client.ObjectKeyFromObject(fg), &cur); err != nil {
			t.Fatal(err)
		}
		return apimeta.FindStatusCondition(cur.Status.Conditions, conditionPITRMysqlConfAccepted)
	}
	refusedEvents := func() int {
		n := 0
		for _, e := range drainRunnerEvents(rec) {
			if strings.Contains(e, "Warning "+eventReasonPITRMysqlConfRefused) {
				n++
			}
		}
		return n
	}
	if err := r.reconcilePITRMySQLConfCondition(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if cond := get(); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonPITRMysqlConfUnsafeRefused ||
		!strings.Contains(cond.Message, "binlog-format=STATEMENT") {
		t.Fatalf("condition %+v", cond)
	}
	if n := refusedEvents(); n != 1 {
		t.Fatalf("%d refusal events, want 1", n)
	}
	if err := r.reconcilePITRMySQLConfCondition(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if n := refusedEvents(); n != 0 {
		t.Fatalf("refusal event repeated for an unchanged set: %d", n)
	}
	fg.Spec.MysqlConf = nil
	if err := r.reconcilePITRMySQLConfCondition(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if cond := get(); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("condition %+v, want True", cond)
	}
	fg.Spec.Backup.PITR.Enabled = false
	if err := r.reconcilePITRMySQLConfCondition(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if cond := get(); cond != nil {
		t.Fatalf("condition %+v kept without PITR", cond)
	}
}

// F003: an enabled archiver that reports no archive identity block (a
// sidecar older than archive identity) is not archiving usably: Unknown
// while its rollout is pending, else False/SidecarOutdated on the second
// poll, never True.
func TestPITRArchiveTrackerOutdatedSidecar(t *testing.T) {
	outdated := pitrObs{status: &internalmysql.ArchiverStatus{Enabled: true, StoreReady: pitrBool(true)}}
	now := time.Now()
	tr := newPITRArchiveTracker(nil)
	tr.observe(now, pitrRound{iad: outdated, pdx: pitrClean()}.results())
	if v := tr.current(); v.Status == metav1.ConditionTrue {
		t.Fatalf("first poll certified an outdated sidecar: %+v", v)
	}
	tr.observe(now.Add(30*time.Second), pitrRound{iad: outdated, pdx: pitrClean()}.results())
	if v := tr.current(); v.Status != metav1.ConditionFalse || v.Reason != reasonPITRSidecarOutdated || !strings.Contains(v.Message, "iad: ") ||
		!strings.Contains(v.Message, "spec.sidecarImage") {
		t.Fatalf("verdict %+v, want False/SidecarOutdated", v)
	}

	rolling := newPITRArchiveTracker(nil)
	round := []archiverPollResult{{site: "iad", status: outdated.status, awaitingRollout: true}, {site: "pdx", status: pitrClean().status}}
	rolling.observe(now, round)
	rolling.observe(now.Add(30*time.Second), round)
	if v := rolling.current(); v.Status != metav1.ConditionUnknown || v.Reason != reasonPITRAwaitingRollout {
		t.Fatalf("verdict %+v, want Unknown/AwaitingRollout while the update is pending", v)
	}
	rolling.observe(now.Add(pitrArchiveRolloutBound+time.Minute), round)
	if v := rolling.current(); v.Status != metav1.ConditionFalse || v.Reason != reasonPITRSidecarOutdated {
		t.Fatalf("verdict %+v, want False/SidecarOutdated after the rollout bound", v)
	}
}

// F146: every archive-identity field change is written at once, and an
// observedTime-only change is not a write by itself.
func TestPITRArchiveWriteNeededArchiveIdentity(t *testing.T) {
	base := func() *v1alpha1.MysqlFailoverGroupStatus {
		obs := metav1.NewMicroTime(time.Unix(100, 0))
		return &v1alpha1.MysqlFailoverGroupStatus{PITR: &v1alpha1.PITRStatus{Enabled: true, ProfileName: "nightly-s3",
			Archive: &v1alpha1.PITRArchiveObservation{Destination: "s3://b/p", ArchiveID: "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa",
				EncryptionMode: v1alpha1.PITRArchiveEncryptionMode(pitrarchive.EncryptionNone), State: v1alpha1.PITRArchiveVerified,
				KeyRevision: "k1", ObservedTime: &obs}}}
	}
	tests := []struct {
		name          string
		mutate        func(a *v1alpha1.PITRArchiveObservation)
		wantImmediate bool
	}{
		{name: "destination", mutate: func(a *v1alpha1.PITRArchiveObservation) { a.Destination = "s3://b/q" }, wantImmediate: true},
		{name: "archive ID", mutate: func(a *v1alpha1.PITRArchiveObservation) { a.ArchiveID = "ar1-bbbbbbbbbbbbbbbbbbbbbbbbbb" }, wantImmediate: true},
		{name: "state", mutate: func(a *v1alpha1.PITRArchiveObservation) { a.State = v1alpha1.PITRArchiveAwaitingDescriptor }, wantImmediate: true},
		{name: "reason", mutate: func(a *v1alpha1.PITRArchiveObservation) {
			a.Reason = v1alpha1.PITRArchiveRefusalReason(pitrarchive.ReasonArchiveForeign)
		}, wantImmediate: true},
		{name: "key revision", mutate: func(a *v1alpha1.PITRArchiveObservation) { a.KeyRevision = "k2" }, wantImmediate: true},
		{name: "encryption mode", mutate: func(a *v1alpha1.PITRArchiveObservation) {
			a.EncryptionMode = v1alpha1.PITRArchiveEncryptionMode(pitrarchive.EncryptionPassphrase)
		}, wantImmediate: true},
		{name: "observed time only", mutate: func(a *v1alpha1.PITRArchiveObservation) {
			later := metav1.NewMicroTime(time.Unix(200, 0))
			a.ObservedTime = &later
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before, after := base(), base()
			tt.mutate(after.PITR.Archive)
			immediate, messageOnly := pitrArchiveWriteNeeded(before, after, []string{"iad", "pdx"})
			if immediate != tt.wantImmediate || messageOnly {
				t.Fatalf("immediate=%t messageOnly=%t, want immediate=%t", immediate, messageOnly, tt.wantImmediate)
			}
		})
	}
	before, after := base(), base()
	after.PITR.Archive = nil
	if immediate, _ := pitrArchiveWriteNeeded(before, after, nil); !immediate {
		t.Fatal("archive block removal not written at once")
	}
}

// D-46: PITRArchiveReissueRiskDetected fires when the active site changes to a site
// whose own manifest holds divergent or quarantined rows, whatever moved it
// (failover, planned failover, re-promotion), and not otherwise.
func TestEmitPITRArchiveReissueRisk(t *testing.T) {
	nn := types.NamespacedName{Namespace: "ns", Name: "lion"}
	rows := func(divergent, quarantined int64) *internalmysql.ArchiverStatus {
		return &internalmysql.ArchiverStatus{Enabled: true, DivergentRows: divergent, QuarantinedRows: quarantined}
	}
	tests := []struct {
		name     string
		from, to string
		pitr     bool
		snap     *internalmysql.ArchiverStatus
		lastGood bool
		// failedOver records an earlier failover on the group.
		failedOver bool
		wantEvent  bool
	}{
		{name: "failover to a site with divergent rows", from: "iad", to: "pdx", pitr: true, snap: rows(2, 0), wantEvent: true},
		{name: "planned failover to a site with quarantined rows", from: "iad", to: "pdx", pitr: true, snap: rows(0, 1), wantEvent: true},
		{name: "re-promotion read from the last good status", from: "iad", to: "pdx", pitr: true, snap: rows(1, 1), lastGood: true, wantEvent: true},
		{name: "clean site", from: "iad", to: "pdx", pitr: true, snap: rows(0, 0)},
		{name: "no promotion", from: "pdx", to: "pdx", pitr: true, snap: rows(3, 0)},
		{name: "bootstrap", from: "", to: "pdx", pitr: true, snap: rows(3, 0)},
		{name: "promotion after a persisted NoPrimary", from: "", to: "pdx", pitr: true, snap: rows(3, 0), failedOver: true, wantEvent: true},
		{name: "PITR disabled", from: "iad", to: "pdx", snap: rows(3, 0)},
		{name: "no archiver status", from: "iad", to: "pdx", pitr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snaps := map[string]*internalmysql.ArchiverStatus{}
			r := newRunnerWithArchiverSnapshots(t, nn, snaps)
			if tt.snap != nil {
				p := r.managers[nn].archiver
				if tt.lastGood {
					p.lastGood = map[string]*internalmysql.ArchiverStatus{tt.to: tt.snap}
				} else {
					p.snapshots[tt.to] = tt.snap
				}
			}
			rec := record.NewFakeRecorder(10)
			r.recorder = rec
			fg := identityFG()
			fg.Spec.Backup.PITR.Enabled = tt.pitr
			fg.Status.ActiveSite = tt.to
			if tt.failedOver {
				at := metav1.Now()
				fg.Status.LastFailover, fg.Status.LastFailoverTarget = &at, "iad"
			}
			r.emitPITRArchiveReissueRisk(fg, tt.from)
			var got []string
			for _, e := range drainRunnerEvents(rec) {
				if strings.Contains(e, eventReasonPITRArchiveReissueRisk) {
					got = append(got, e)
				}
			}
			if tt.wantEvent != (len(got) == 1) {
				t.Fatalf("events %v, want reissue warning=%t", got, tt.wantEvent)
			}
			if tt.wantEvent && (!strings.HasPrefix(got[0], "Warning") || !strings.Contains(got[0], "site pdx was promoted") || !strings.Contains(got[0], "PITRArchiveHistoryReset")) {
				t.Fatalf("event text: %s", got[0])
			}
		})
	}
}

// C3: every Event reason #206 adds follows A-74: <Subject><PastTenseVerb>,
// PITR as the only acronym form, and never the PointInTime prefix.
func TestPITREventReasonsFollowNamingRule(t *testing.T) {
	rule := regexp.MustCompile(`^PITR[A-Z][A-Za-z]*(Detected|Excluded|Refused|Skipped|Truncated)$`)
	for _, reason := range []string{
		eventReasonPITRArchiveReissueRisk,
		eventReasonPITRArchiveUnsafeHistory,
		eventReasonPITRMysqlConfRefused,
		eventReasonPITRDivergentHistoryExcluded,
		eventReasonPITROnHistoryTransactionsExcluded,
	} {
		if !rule.MatchString(reason) {
			t.Errorf("Event reason %q breaks A-74's <Subject><PastTenseVerb> rule", reason)
		}
	}
}
