package controller

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"reflect"
	"slices"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// MysqlDatabaseFinalizer guards the MySQL-side cleanup decision. Named to
// match the existing finalizers in this repo (shipstream.io/mysqlbackup,
// shipstream.io/mysqlbackup-verification).
const MysqlDatabaseFinalizer = "shipstream.io/mysqldatabase"

// ConditionDatabaseReady is the Ready condition type on
// MysqlDatabase.status.conditions. Together with status.observedGeneration
// it is the contract a caller polls instead of opening a MySQL connection,
// so treat both as API surface rather than as diagnostics.
const ConditionDatabaseReady = "Ready"

const (
	mysqlDatabasePendingRequeue = 30 * time.Second
	mysqlDatabaseFailedRequeue  = 60 * time.Second
)

// mysqlDatabaseReconcileBudget bounds the MySQL work of a single reconcile.
// A maximally-sized CR (64 grants) is ~200 serial round-trips, each bounded
// only by the driver's per-statement timeout; without an overall budget one
// slow-but-responsive primary could occupy a reconcile worker for a quarter
// of an hour and stall every other tenant's rotation and finalizer. Hitting
// the budget is classified transient — the work simply continues next time.
const mysqlDatabaseReconcileBudget = 60 * time.Second

// mysqlDatabaseMaxConcurrentReconciles lets distinct tenant CRs reconcile in
// parallel. Reconciles of different CRs are independent: ownershipConflict
// arbitrates same-database authority deterministically by rank, and
// controller-runtime already deduplicates the same key.
const mysqlDatabaseMaxConcurrentReconciles = 4

// errGrantUserMissing reports a spec.grants[] entry naming a MySQL user that
// does not exist at host '%'. It is a distinct type because the response is
// specific: fail the CR loudly with reason GrantUserMissing and, above all,
// do not create the user. A MysqlDatabase that could conjure arbitrary MySQL
// principals would be a privilege-escalation primitive.
type errGrantUserMissing struct {
	username string
}

func (e *errGrantUserMissing) Error() string {
	return fmt.Sprintf("MySQL user %q does not exist at host '%%' (grants[] matches host '%%' only, same as every account Bloodraven creates); spec.grants[] never creates users", e.username)
}

// errDatabasePreExists reports a spec.databaseName that already exists on
// the group without this CR's status.databaseCreated stamp. Adopting it
// would grant tenant principals onto a schema Bloodraven does not own and
// could later authorize a DROP of someone else's data, so the CR fails
// closed instead.
type errDatabasePreExists struct {
	database string
}

func (e *errDatabasePreExists) Error() string {
	return fmt.Sprintf("database %q already exists on the group and this CR did not create it (status.databaseCreated is false); refusing to adopt a schema Bloodraven does not own", e.database)
}

// errPreExistingOwnerUser reports an owner account (one user@host) that
// already exists in MySQL without being attributed to this CR by any of its
// pre-reconcile records for that exact host. CREATE USER IF NOT EXISTS +
// ALTER USER would otherwise reset the password of an account some other
// system created — and a later deletionPolicy: Delete would drop it.
type errPreExistingOwnerUser struct {
	username string
	host     string
}

func (e *errPreExistingOwnerUser) Error() string {
	return fmt.Sprintf("MySQL account '%s'@'%s' already exists and is not recorded as this CR's account (status.ownerUser/pendingOwnerUser/appliedUsers with that host); refusing to manage an account Bloodraven did not create", e.username, e.host)
}

// errPreExistingUser is errPreExistingOwnerUser for a spec.users[] entry: the
// account exists in MySQL but none of this CR's pre-reconcile records
// attributes that user@host to it, so CREATE USER IF NOT EXISTS + ALTER USER
// would reset the password of an account something else created — including
// a sibling CR's owner or users[] principal.
type errPreExistingUser struct {
	secretName string
	username   string
	host       string
}

func (e *errPreExistingUser) Error() string {
	return fmt.Sprintf("MySQL account '%s'@'%s' already exists and is not attributed to this CR by its status records (ownerUser/pendingOwnerUser/appliedUsers with that host); refusing to manage an account Bloodraven did not create for spec.users[] entry %q", e.username, e.host, e.secretName)
}

// tenantUserInput pairs a spec.users[] entry with the credential its Secret
// currently carries. Constructed only after every users[] Secret resolved —
// a missing or incomplete Secret parks the whole CR Pending first, mirroring
// the owner Secret (one lagging ESO sync therefore delays unrelated spec
// changes on the same CR; that is the documented trade for never applying a
// partial users[] list).
type tenantUserInput struct {
	entry    v1alpha1.MysqlDatabaseUser
	username string
	password string
	hosts    []string
	secret   *corev1.Secret
}

// managedPrincipal is a username over the hosts its accounts may exist on —
// the unit the delete path drops.
type managedPrincipal struct {
	username string
	hosts    []string
}

func principalNames(ps []managedPrincipal) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.username)
	}
	return out
}

// transientSQLError distinguishes connectivity weather from a MySQL verdict.
// It exists because an unplanned failover can land between the dial and the
// exec: the group watch re-enqueues every tenant the moment ActiveSite moves,
// which can be up to 30s before the promoted site is actually writable
// (pendingPromotionActiveSiteTTL). DDL against that primary fails with 1290;
// a connection killed mid-promotion surfaces as a driver/net error. Neither
// is a fact about the CR, so neither may latch Phase=Failed — a healthy
// tenant must not go red for every ordinary failover. The reconcile budget
// expiring is weather too: the MySQL work simply did not finish in time.
func transientSQLError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var myErr *mysqldriver.MySQLError
	if errors.As(err, &myErr) {
		// 1290: ER_OPTION_PREVENTS_STATEMENT (super_read_only primary,
		// i.e. promotion not finished). 1836: ER_READ_ONLY_MODE.
		return myErr.Number == 1290 || myErr.Number == 1836
	}
	var netErr net.Error
	return errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, mysqldriver.ErrInvalidConn) ||
		errors.Is(err, io.EOF) ||
		errors.As(err, &netErr)
}

// MysqlDatabaseReconciler reconciles MysqlDatabase resources: one tenant
// database, its owning user, and grant-only entries for principals that
// already exist.
//
// The security property this controller exists to provide: a caller holding
// only RBAC on mysqldatabases in one namespace can provision a tenant
// database while holding no MySQL credential and no Secret access. Any change
// here that would require the caller to hold either is the wrong change.
type MysqlDatabaseReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// OpenDB overrides the MySQL dialer. Production leaves it nil, which
	// selects openMySQL — the same dialer reconcileCredentials uses.
	// Component tests substitute an in-memory MySQL model so they exercise
	// the real reconciler against fake SQL rather than a fake reconciler.
	OpenDB openMySQLFunc
}

// The operator gets no create and no delete on mysqldatabases: it never
// invents tenant databases, only reconciles ones a caller declared. Callers
// get create/delete through a separate namespaced Role that confers no
// Secret access and no MySQL credential — see
// config/rbac/mysqldatabase_caller_role.yaml.
//
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqldatabases,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqldatabases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqldatabases/finalizers,verbs=update

func (r *MysqlDatabaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("mysqldatabase", req.NamespacedName)

	var mdb v1alpha1.MysqlDatabase
	if err := r.Get(ctx, req.NamespacedName, &mdb); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !mdb.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &mdb)
	}

	if !controllerutil.ContainsFinalizer(&mdb, MysqlDatabaseFinalizer) {
		controllerutil.AddFinalizer(&mdb, MysqlDatabaseFinalizer)
		if err := r.Update(ctx, &mdb); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Resolve the group. A MysqlDatabase applied before its group is a
	// normal ordering, not a fault, so this is Pending rather than Failed.
	var fg v1alpha1.MysqlFailoverGroup
	fgKey := types.NamespacedName{Namespace: mdb.Namespace, Name: mdb.Spec.GroupRef.Name}
	if err := r.Get(ctx, fgKey, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			return r.pending(ctx, &mdb, "GroupNotFound",
				fmt.Sprintf("MysqlFailoverGroup %q not found in namespace %s", fgKey.Name, mdb.Namespace))
		}
		return ctrl.Result{}, fmt.Errorf("get failover group: %w", err)
	}

	// A terminating group is about to lose its primary; starting new tenant
	// DDL on it would create state the teardown then has to reason about.
	// Wait — the group disappearing entirely is handled by GroupNotFound.
	if !fg.DeletionTimestamp.IsZero() {
		return r.pending(ctx, &mdb, "GroupTerminating",
			fmt.Sprintf("MysqlFailoverGroup %q is being deleted; not applying new tenant DDL", fg.Name))
	}

	// Gate on an active site, mirroring reconcileCredentials.
	if fg.Status.ActiveSite == "" {
		return r.pending(ctx, &mdb, "NoActiveSite",
			fmt.Sprintf("MysqlFailoverGroup %q has no active site yet", fg.Name))
	}

	// In-place restore and planned failover both fence the primary. Backing
	// off to Pending is deliberate: erroring here would surface a red CR for
	// a maintenance window that is working as designed.
	if reason, msg, fenced := groupFenced(&fg); fenced {
		return r.pending(ctx, &mdb, reason, msg)
	}

	// The owner Secret is written by the caller (in ShipStream's case,
	// rendered by ESO from OpenBao). Its absence is another ordering
	// problem, not a spec error.
	var ownerSecret corev1.Secret
	secretKey := types.NamespacedName{Namespace: mdb.Namespace, Name: mdb.Spec.Owner.SecretName}
	if err := r.Get(ctx, secretKey, &ownerSecret); err != nil {
		if apierrors.IsNotFound(err) {
			return r.pending(ctx, &mdb, "OwnerSecretMissing",
				fmt.Sprintf("Secret %q not found in namespace %s", secretKey.Name, mdb.Namespace))
		}
		return ctrl.Result{}, fmt.Errorf("get owner secret: %w", err)
	}
	ownerUser := string(ownerSecret.Data["username"])
	ownerPass := string(ownerSecret.Data["password"])
	ownerHosts := mdb.Spec.Owner.EffectiveHosts()
	if ownerUser == "" || ownerPass == "" {
		return r.pending(ctx, &mdb, "OwnerSecretIncomplete",
			fmt.Sprintf("Secret %q must carry non-empty username and password keys", secretKey.Name))
	}

	// The users[] Secrets follow the exact contract of the owner Secret,
	// including the ordering tolerance: all of them are read before any SQL
	// is considered, and any missing or incomplete one parks the whole CR
	// Pending (KV write → ESO render → CR apply self-heals; a partial
	// users[] list is never applied).
	users := make([]tenantUserInput, 0, len(mdb.Spec.Users))
	userUsernames := make(map[string]string, len(mdb.Spec.Users))
	for _, entry := range mdb.Spec.Users {
		var userSecret corev1.Secret
		userKey := types.NamespacedName{Namespace: mdb.Namespace, Name: entry.SecretName}
		if err := r.Get(ctx, userKey, &userSecret); err != nil {
			if apierrors.IsNotFound(err) {
				return r.pending(ctx, &mdb, "UserSecretMissing",
					fmt.Sprintf("spec.users[] Secret %q not found in namespace %s", entry.SecretName, mdb.Namespace))
			}
			return ctrl.Result{}, fmt.Errorf("get users[] secret %q: %w", entry.SecretName, err)
		}
		username := string(userSecret.Data["username"])
		password := string(userSecret.Data["password"])
		if username == "" || password == "" {
			return r.pending(ctx, &mdb, "UserSecretIncomplete",
				fmt.Sprintf("spec.users[] Secret %q must carry non-empty username and password keys", entry.SecretName))
		}
		users = append(users, tenantUserInput{entry: entry, username: username, password: password, hosts: entry.EffectiveHosts(), secret: &userSecret})
		userUsernames[entry.SecretName] = username
	}

	// Validate everything that ends up in SQL before anything is rendered.
	// The owner and users[] usernames in particular cannot be validated by
	// the API server, because they arrive from Secrets.
	if err := mdb.Spec.Validate(ownerUser, userUsernames); err != nil {
		return r.fail(ctx, &mdb, "InvalidSpec", err.Error())
	}

	// Refuse to adopt a group-level principal as a tenant owner. Both this
	// gate and the ownership arbitration below run BEFORE the hash
	// short-circuit: a Ready CR whose world changed underneath it (a new
	// higher-ranked peer appeared, a group Secret rotated into its
	// username) must be re-arbitrated on every reconcile, not skipped
	// because its inputs hash the same. The reads are cache-local; the
	// correctness of the custody model is worth more than the skip.
	//
	// The check fails closed: a group Secret that is mid-rotation
	// (NotFound) parks the tenant in Pending, and any other read error
	// fails the reconcile — never proceed on a partial reserved set. See
	// reservedGroupUsernames.
	reserved, err := reservedGroupUsernames(ctx, r.Client, &fg)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.pending(ctx, &mdb, "GroupSecretMissing",
				fmt.Sprintf("cannot resolve group %q's reserved usernames: %v", fg.Name, err))
		}
		return ctrl.Result{}, fmt.Errorf("resolve reserved group usernames: %w", err)
	}
	if reserved[ownerUser] {
		return r.fail(ctx, &mdb, "OwnerUserReserved", fmt.Sprintf(
			"Secret %q names owner user %q, which is a credential of MysqlFailoverGroup %q; "+
				"a MysqlDatabase owns only its own tenant user and must not set the password of a group-level principal",
			secretKey.Name, ownerUser, fg.Name))
	}
	// The same gate for every users[] entry, for the same reason, in the
	// same pre-hash position: without it, a caller-controlled Secret naming
	// `replicator` would ALTER USER the group's replication credential from
	// a tenant CR — the one way users[] could become the escalation
	// primitive this CRD exists not to be.
	for _, u := range users {
		if reserved[u.username] {
			return r.fail(ctx, &mdb, "UserReserved", fmt.Sprintf(
				"spec.users[] Secret %q names user %q, which is a credential of MysqlFailoverGroup %q; "+
					"a MysqlDatabase manages only its own tenant principals and must not set the password of a group-level principal",
				u.entry.SecretName, u.username, fg.Name))
		}
	}

	// Refuse to fight another CR over the same database or the same owner
	// principal. Without this, deleting a duplicate CR that carries
	// deletionPolicy: Delete would DROP the survivor's live database, and two
	// CRs sharing an owner Secret would take turns resetting each other's
	// password. Oldest CR wins; the newer one fails loudly.
	conflictKind, conflictReason, conflictMsg, err := r.ownershipConflict(ctx, &mdb, ownerUser)
	if err != nil {
		return ctrl.Result{}, err
	}
	switch conflictKind {
	case conflictVerdictFail:
		return r.fail(ctx, &mdb, conflictReason, conflictMsg)
	case conflictVerdictPending:
		return r.pending(ctx, &mdb, conflictReason, conflictMsg)
	}

	currentHash, err := computeDatabaseHash(&mdb, &ownerSecret, users, &fg)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("compute database hash: %w", err)
	}

	// Skip if nothing that matters changed. The active site and the group's
	// identity are part of the hash, so a failover — or a recreated group,
	// or a completed in-place restore — invalidates it and forces a
	// re-apply, which is why the group watch is correct rather than merely
	// helpful.
	if mdb.Status.Phase == v1alpha1.MysqlDatabasePhaseReady &&
		mdb.Status.LastAppliedHash == currentHash &&
		mdb.Status.ObservedGeneration == mdb.Generation {
		return ctrl.Result{}, nil
	}

	// Snapshot the write-ahead record as it stood before this reconcile.
	// Attribution judges "did this CR create that account" by what was
	// recorded before this run, so prior stays the untouched pre-reconcile
	// snapshot — the only exception is dropStaleRotationTargets forgetting
	// records whose accounts it dropped or vetoed.
	prior := mdb.Status.DeepCopy()

	// users[] ownership arbitration: a username this CR has not yet
	// recorded is refused when a live sibling on the group already claims
	// it — as its owner, through its spec.grants[], or through its own
	// ledger. The MySQL existence check below is not an ownership lock: a
	// sibling that stamped its write-ahead and then crashed (or is mid-
	// reconcile) has a persisted claim on an account that does not exist
	// yet, and without this gate both CRs would create it, record it, and
	// take turns resetting its password. The claim is read before this
	// CR's own write-ahead stamp, so the first persisted record wins and
	// the loser runs no SQL; the residual window is informer-cache lag on
	// a simultaneous first claim (see the known gaps in the docs).
	if len(users) > 0 {
		var siblings v1alpha1.MysqlDatabaseList
		if err := r.List(ctx, &siblings, client.InNamespace(mdb.Namespace)); err != nil {
			return ctrl.Result{}, fmt.Errorf("list mysqldatabases for users[] claim guard: %w", err)
		}
		for _, u := range users {
			if ownNameRecorded(prior, u.username) {
				continue
			}
			if referrer, how, claimed := siblingPrincipalClaimIn(&siblings, mdb.Name, mdb.Spec.GroupRef.Name, u.username); claimed {
				return r.fail(ctx, &mdb, "UserClaimedBySibling", fmt.Sprintf(
					"spec.users[] Secret %q names user %q, which MysqlDatabase %q already claims (%s); "+
						"a users[] principal belongs to exactly one MysqlDatabase — share a group-level principal through spec.grants[] instead",
					u.entry.SecretName, u.username, referrer, how))
			}
		}
	}

	// Budget every MySQL interaction: one slow primary must not be able to
	// occupy the worker indefinitely (see mysqlDatabaseReconcileBudget).
	sqlCtx, cancel := context.WithTimeout(ctx, mysqlDatabaseReconcileBudget)
	defer cancel()

	db, err := openAdminConnection(sqlCtx, r.Client, &fg, r.dialer())
	if err != nil {
		// Connectivity is transient by nature; stay Pending and let the
		// controller back off rather than declaring the tenant broken.
		logger.V(1).Info("admin connection unavailable", "error", err)
		return r.pending(ctx, &mdb, "PrimaryUnavailable",
			fmt.Sprintf("cannot reach the primary of group %q: %v", fg.Name, err))
	}
	defer db.Close()

	// Every username this CR's current spec declares, and on which surface.
	// Every drop below consults it: a name that is still desired — even on
	// a different surface than the record being retired — is never dropped.
	claims := currentPrincipalClaims(ownerUser, ownerHosts, mdb.Spec.Grants, users)

	// Rotation targets the ledger still records as pending but the Secrets
	// no longer name are dropped now, before this reconcile's write-ahead
	// overwrites those records — see dropStaleRotationTargets.
	if res, done, err := r.dropStaleRotationTargets(ctx, sqlCtx, db, &mdb, &fg, reserved, claims, ownerUser, users, prior); done {
		return res, err
	}

	// Every adoption gate runs before anything is written ahead. A record
	// may name user@host only if that exact account was verified absent
	// here or was already attributed to this CR — so a refusal, or a query
	// that fails, leaves no speculative record behind for a later reconcile
	// to trust.
	dbExists, err := preflightAdoption(sqlCtx, db, &mdb, ownerUser, ownerHosts, users, prior)
	if err != nil {
		return r.applyFailed(ctx, &mdb, &fg, err)
	}

	// The single write-ahead stamp: DatabaseCreated, the owner record and
	// the users[] ledger are committed after the preflight and before any
	// SQL executes, so a partially-applied CR still knows what it may have
	// touched when deletionPolicy: Delete needs to clean up. During a
	// username rotation the previous name stays recorded until its account
	// is actually dropped; the new one is recorded as pending, each with
	// its own hosts.
	if err := r.stampStatus(ctx, &mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
		st.Phase = v1alpha1.MysqlDatabasePhaseCreating
		st.ObservedGeneration = mdb.Generation
		st.DatabaseCreated = true
		carried := replacedPendingHosts(st, ownerUser, users)
		stampOwnerWriteAhead(st, ownerUser, ownerHosts, carried)
		stampUsersWriteAhead(st, users, carried)
		st.Message = fmt.Sprintf("applying database %s on site %s", mdb.Spec.DatabaseName, fg.Status.ActiveSite)
		setCondition(&st.Conditions, metav1.Condition{
			Type:               ConditionDatabaseReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: mdb.Generation,
			LastTransitionTime: metav1.Now(),
			Reason:             "Applying",
			Message:            st.Message,
		})
	}); err != nil {
		return ctrl.Result{}, err
	}

	appliedGrants, progress, err := applyDatabase(sqlCtx, db, &mdb, ownerUser, ownerPass, ownerHosts, users, prior, dbExists)
	if err != nil {
		// Records written ahead for a principal none of whose statements
		// executed describe nothing this CR touched: withdraw them, or the
		// next reconcile would trust them to adopt (or Delete to drop) an
		// account created in the meantime by someone else.
		if werr := r.stampStatus(ctx, &mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
			withdrawUnexecuted(st, prior, progress, users)
		}); werr != nil {
			return ctrl.Result{}, werr
		}
		return r.applyFailed(ctx, &mdb, &fg, err)
	}

	// A rotated owner username means the previously-recorded account is
	// obsolete desired state. The drop happens now — after the new owner is
	// created, granted, and applied — so rotation is create-before-drop: a
	// failure mid-handover leaves both accounts alive and retried, never a
	// window in which the tenant has no owner at all. status kept the old
	// name through the apply, so a transient failure here retries rather
	// than leaking the account. It loses its rights on this database first
	// (even when the drop is vetoed), and it is dropped only on the hosts
	// recorded for that name.
	if prev := prior.OwnerUser; prev != "" && prev != ownerUser {
		if c, ok := claims[prev]; ok && c.managed {
			logger.V(1).Info("previous owner user is now declared by this MysqlDatabase; not dropping",
				"previousOwner", prev, "claimedBy", c.surface)
		} else if res, done, err := r.retirePrincipal(ctx, sqlCtx, db, &mdb, &fg, reserved, claims, retirement{
			kind:     principalKindOwner,
			username: prev,
			hosts:    recordedOwnerHosts(prior),
			revoke:   true,
			why:      fmt.Sprintf("during rotation to %q", ownerUser),
			what:     "previous owner user",
			dropped: func() {
				r.Recorder.Eventf(&mdb, corev1.EventTypeNormal, "OwnerUserRotated",
					"created and granted new owner user %q, then dropped previous owner user %q", ownerUser, prev)
			},
		}); done {
			return res, err
		}
	}

	// Rotated users[] entries follow the owner's create-before-drop
	// contract: every name the ledger records for the entry other than the
	// current one is obsolete desired state — unless a current surface of
	// this CR declares it, it became reserved, or a sibling claims it.
	for _, u := range users {
		prevState := findAppliedUserIn(prior.AppliedUsers, u.entry.SecretName)
		if prevState == nil {
			continue
		}
		for _, prev := range ledgerUsernames(*prevState) {
			if prev == u.username {
				continue
			}
			if c, ok := claims[prev]; ok && c.managed {
				// Another current surface took the name this entry
				// rotated away from; applyDatabase just re-applied it
				// there. Dropping it would destroy live desired state.
				logger.V(1).Info("previous users[] principal is now claimed by another entry; not dropping",
					"previousUsername", prev, "fromSecret", u.entry.SecretName, "toSecret", c.surface)
				continue
			}
			secretName, current := u.entry.SecretName, u.username
			prevName := prev
			if res, done, err := r.retirePrincipal(ctx, sqlCtx, db, &mdb, &fg, reserved, claims, retirement{
				kind:     principalKindUser,
				username: prev,
				hosts:    ledgerHostsForName(*prevState, prev),
				why:      fmt.Sprintf("during rotation of spec.users[] entry %q to %q", secretName, current),
				what:     "previous users[] principal",
				dropped: func() {
					r.Recorder.Eventf(&mdb, corev1.EventTypeNormal, "UserRotated",
						"created and granted users[] principal %q (secret %q), then dropped previous principal %q",
						current, secretName, prevName)
				},
			}); done {
				return res, err
			}
		}
	}

	// Ledger entries whose secretName has left spec.users[] are removed
	// from MySQL: users[] is desired state on the way out as well as on the
	// way in — no surveyed operator drops list-removed users, and that
	// orphaning is exactly wrong for a per-tenant support credential. The
	// revoke always runs (a vetoed drop must still lose its rights on this
	// database); the drop itself is vetted like every other drop. The
	// ledger record — not the Secret, which may already be gone — is what
	// names the account.
	for i := range prior.AppliedUsers {
		state := prior.AppliedUsers[i]
		if specHasUserSecret(&mdb.Spec, state.SecretName) {
			continue
		}
		for _, name := range ledgerUsernames(state) {
			hosts := ledgerHostsForName(state, name)
			if c, claimed := claims[name]; claimed {
				// The entry left spec but its account did not: a current
				// surface declares the same username (a Secret rename, or a
				// move to the owner or to grants[]). applyDatabase re-applied
				// it there, so neither the revoke nor the drop may run on
				// the accounts that surface manages. Surface the handover so
				// an operator can see why no UserRemoved fired.
				if c.managed {
					hosts = nil
				} else {
					hosts = withoutHost(hosts, tenantUserHost)
				}
				if len(hosts) == 0 {
					r.Recorder.Eventf(&mdb, corev1.EventTypeNormal, "UserTransferred",
						"spec.users[] entry %q was removed but principal %q is now declared by %s; kept the account",
						state.SecretName, name, c.surface)
					continue
				}
			}
			secretName, removed := state.SecretName, name
			if res, done, err := r.retirePrincipal(ctx, sqlCtx, db, &mdb, &fg, reserved, claims, retirement{
				kind:     principalKindUser,
				username: name,
				hosts:    hosts,
				revoke:   true,
				why:      fmt.Sprintf("after removal of spec.users[] entry %q", secretName),
				what:     "removed users[] principal",
				dropped: func() {
					r.Recorder.Eventf(&mdb, corev1.EventTypeNormal, "UserRemoved",
						"spec.users[] entry %q was removed: revoked and dropped principal %q", secretName, removed)
				},
			}); done {
				return res, err
			}
		}
	}

	// Hosts no longer declared for a principal that stays: every host any
	// pre-reconcile record attributes to the current owner or users[]
	// username (its settled hosts, an in-flight rotation target's hosts, or
	// a record transferred from another surface) but the spec no longer
	// lists is obsolete desired state, dropped on exactly those hosts —
	// otherwise the Ready stamp would settle the record and orphan them.
	if res, done, err := r.dropUndeclaredHosts(ctx, sqlCtx, db, &mdb, &fg, "spec.owner secret username", ownerUser, ownerHosts, prior, func(removed []string) {
		r.Recorder.Eventf(&mdb, corev1.EventTypeNormal, "OwnerHostsRemoved",
			"dropped owner user %q on hosts no longer declared: %s", ownerUser, strings.Join(removed, ", "))
	}); done {
		return res, err
	}
	for _, u := range users {
		if res, done, err := r.dropUndeclaredHosts(ctx, sqlCtx, db, &mdb, &fg, "spec.users[] ledger username", u.username, u.hosts, prior, func(removed []string) {
			r.Recorder.Eventf(&mdb, corev1.EventTypeNormal, "UserHostsRemoved",
				"dropped users[] principal %q (secret %q) on hosts no longer declared: %s",
				u.username, u.entry.SecretName, strings.Join(removed, ", "))
		}); done {
			return res, err
		}
	}

	message := fmt.Sprintf("database %s ready on site %s", mdb.Spec.DatabaseName, fg.Status.ActiveSite)
	if err := r.stampStatus(ctx, &mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
		st.Phase = v1alpha1.MysqlDatabasePhaseReady
		st.ObservedGeneration = mdb.Generation
		st.DatabaseCreated = true
		st.OwnerUser = ownerUser
		st.OwnerHosts = unionHosts(nil, ownerHosts)
		st.PendingOwnerUser = ""
		st.PendingOwnerHosts = nil
		st.AppliedGrants = appliedGrants
		st.AppliedUsers = readyAppliedUsers(users)
		st.ActiveSite = fg.Status.ActiveSite
		st.LastAppliedHash = currentHash
		st.Message = message
		setCondition(&st.Conditions, metav1.Condition{
			Type:               ConditionDatabaseReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: mdb.Generation,
			LastTransitionTime: metav1.Now(),
			Reason:             "DatabaseReconciled",
			Message:            message,
		})
	}); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("reconciled tenant database",
		"database", mdb.Spec.DatabaseName, "group", fg.Name, "site", fg.Status.ActiveSite)
	return ctrl.Result{}, nil
}

// applyFailed maps an adoption refusal or apply error to the CR's phase:
// the refusals and GrantUserMissing are verdicts about the CR, connectivity
// and read-only errors are weather (Pending), anything else is a MySQL
// verdict about the CR's own statements.
func (r *MysqlDatabaseReconciler) applyFailed(ctx context.Context, mdb *v1alpha1.MysqlDatabase, fg *v1alpha1.MysqlFailoverGroup, err error) (ctrl.Result, error) {
	var preExists *errDatabasePreExists
	if errors.As(err, &preExists) {
		return r.fail(ctx, mdb, "DatabasePreExists", err.Error())
	}
	var preUser *errPreExistingOwnerUser
	if errors.As(err, &preUser) {
		return r.fail(ctx, mdb, "PreExistingOwnerUser", err.Error())
	}
	var preTenantUser *errPreExistingUser
	if errors.As(err, &preTenantUser) {
		return r.fail(ctx, mdb, "PreExistingUser", err.Error())
	}
	var missing *errGrantUserMissing
	if errors.As(err, &missing) {
		return r.fail(ctx, mdb, "GrantUserMissing", err.Error())
	}
	if transientSQLError(err) {
		log.FromContext(ctx).WithValues("mysqldatabase", client.ObjectKeyFromObject(mdb)).V(1).Info("transient MySQL error, staying pending", "error", err)
		return r.pending(ctx, mdb, "PrimaryUnavailable",
			fmt.Sprintf("transient MySQL error on group %q: %v", fg.Name, err))
	}
	return r.fail(ctx, mdb, "MySQLError", err.Error())
}

// reconcileDelete releases the finalizer, dropping MySQL state only under an
// explicit deletionPolicy: Delete.
func (r *MysqlDatabaseReconciler) reconcileDelete(ctx context.Context, mdb *v1alpha1.MysqlDatabase) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(mdb, MysqlDatabaseFinalizer) {
		return ctrl.Result{}, nil
	}

	// Retain is the default, and the default is the whole point: a CR
	// garbage-collected by a GitOps prune, a namespace delete or a bad
	// selector must not take a live tenant database with it.
	if mdb.Spec.EffectiveDeletionPolicy() == v1alpha1.MysqlDatabaseRetain {
		r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "DatabaseRetained",
			"deletionPolicy=Retain: database %q and user %q left untouched in MySQL",
			mdb.Spec.DatabaseName, mdb.Status.OwnerUser)
		return ctrl.Result{}, r.removeFinalizer(ctx, mdb)
	}

	// DatabaseCreated is the write-ahead record from the apply path: it is
	// stamped once the admin connection is open, before the first statement
	// executes. A CR without it (invalid spec, reserved owner, ownership
	// conflict, unreachable primary — all fail before SQL) has nothing of
	// its own in MySQL, and must not drop a database that some other CR or
	// system created under the same name.
	if !mdb.Status.DatabaseCreated {
		r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "DatabaseDropSkipped",
			"deletionPolicy=Delete: this CR never applied any DDL for database %q; nothing to drop", mdb.Spec.DatabaseName)
		return ctrl.Result{}, r.removeFinalizer(ctx, mdb)
	}

	logger := log.FromContext(ctx)

	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseDeleting {
		if err := r.stampStatus(ctx, mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
			st.Phase = v1alpha1.MysqlDatabasePhaseDeleting
			st.Message = fmt.Sprintf("dropping database %s", mdb.Spec.DatabaseName)
			setCondition(&st.Conditions, metav1.Condition{
				Type:               ConditionDatabaseReady,
				Status:             metav1.ConditionFalse,
				ObservedGeneration: mdb.Generation,
				LastTransitionTime: metav1.Now(),
				Reason:             "Deleting",
				Message:            st.Message,
			})
		}); err != nil {
			return ctrl.Result{}, err
		}
	}

	var fg v1alpha1.MysqlFailoverGroup
	fgKey := types.NamespacedName{Namespace: mdb.Namespace, Name: mdb.Spec.GroupRef.Name}
	if err := r.Get(ctx, fgKey, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			// Nothing to connect to. Mirrors the backup reconciler's
			// ArtifactCleanupSkipped: release rather than wedge the CR.
			r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseCleanupSkipped",
				"MysqlFailoverGroup %q is gone; cannot drop database %q", fgKey.Name, mdb.Spec.DatabaseName)
			return ctrl.Result{}, r.removeFinalizer(ctx, mdb)
		}
		return ctrl.Result{}, fmt.Errorf("get failover group: %w", err)
	}

	if fg.Status.ActiveSite == "" {
		// A requested DROP is not silently skipped. The CR waits. The
		// escape hatch is to patch spec.deletionPolicy to Retain, which
		// is a deliberate human decision rather than a default.
		r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseDropDeferred",
			"group %q has no active site; deferring DROP of database %q", fg.Name, mdb.Spec.DatabaseName)
		return ctrl.Result{RequeueAfter: pendingRequeueDelay()}, nil
	}

	// The apply path backs off while the primary is fenced; injecting a
	// DROP DATABASE into an in-place restore or a planned failover's drain
	// window would be strictly worse than injecting a CREATE.
	if _, msg, fenced := groupFenced(&fg); fenced {
		r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseDropDeferred",
			"%s; deferring DROP of database %q", msg, mdb.Spec.DatabaseName)
		return ctrl.Result{RequeueAfter: pendingRequeueDelay()}, nil
	}

	// The apply path refuses to drop an owner that has since become a
	// group-level principal; the delete path must refuse identically, or a
	// group credential created after the CR's last apply would be dropped
	// when the CR goes away. Same fail-closed contract as the apply path:
	// an unreadable group Secret defers the DROP rather than proceeding on
	// a partial reserved set.
	reserved, err := reservedGroupUsernames(ctx, r.Client, &fg)
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseDropDeferred",
				"cannot resolve group %q's reserved usernames: %v; deferring DROP of database %q",
				fg.Name, err, mdb.Spec.DatabaseName)
			return ctrl.Result{RequeueAfter: pendingRequeueDelay()}, nil
		}
		return ctrl.Result{}, fmt.Errorf("resolve reserved group usernames: %w", err)
	}

	// Scope the drop to what is exclusively ours. Another live CR declaring
	// the same database (a conflict that predates its own failure, or a
	// mid-migration duplicate) means the schema is not ours to drop; another
	// live CR claiming the same owner principal means the user is not.
	dropDB, dropOwners, err := r.deleteScope(ctx, mdb, reserved)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !dropDB && len(dropOwners) == 0 {
		return ctrl.Result{}, r.removeFinalizer(ctx, mdb)
	}

	// Budget the MySQL work exactly like the apply path.
	sqlCtx, cancel := context.WithTimeout(ctx, mysqlDatabaseReconcileBudget)
	defer cancel()

	db, err := openAdminConnection(sqlCtx, r.Client, &fg, r.dialer())
	if err != nil {
		// Never a hard error: a CR wedged in Deleting with no breadcrumb is
		// worse than one that visibly waits. The DROP is still requested —
		// the CR simply retries on the pending interval.
		r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseDropDeferred",
			"cannot reach the primary of group %q: %v; deferring DROP of database %q",
			fg.Name, err, mdb.Spec.DatabaseName)
		if serr := r.stampStatus(ctx, mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
			st.Message = fmt.Sprintf("dropping database %s: waiting for the primary of group %q to become reachable", mdb.Spec.DatabaseName, fg.Name)
		}); serr != nil {
			return ctrl.Result{}, serr
		}
		return ctrl.Result{RequeueAfter: pendingRequeueDelay()}, nil
	}
	defer db.Close()

	if err := dropDatabase(sqlCtx, db, mdb, dropDB, dropOwners); err != nil {
		if transientSQLError(err) {
			r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseDropDeferred",
				"transient MySQL error dropping database %q on group %q: %v; retrying", mdb.Spec.DatabaseName, fg.Name, err)
			if serr := r.stampStatus(ctx, mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
				st.Message = fmt.Sprintf("dropping database %s: transient MySQL error, retrying", mdb.Spec.DatabaseName)
			}); serr != nil {
				return ctrl.Result{}, serr
			}
			return ctrl.Result{RequeueAfter: pendingRequeueDelay()}, nil
		}
		return ctrl.Result{}, fmt.Errorf("drop tenant database: %w", err)
	}

	// The DROP went to the primary we snapshotted. If the group moved or
	// became fenced while we were executing, the statement may have hit a
	// stale primary: keep the finalizer and re-verify instead of finalizing
	// on an assumption. (DDL replicates, so an intact group converges; the
	// recheck catches the failover-mid-DROP race, not normal replication.)
	var refreshed v1alpha1.MysqlFailoverGroup
	if err := r.Get(ctx, fgKey, &refreshed); err != nil {
		if apierrors.IsNotFound(err) {
			r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseCleanupSkipped",
				"MysqlFailoverGroup %q disappeared during the DROP of database %q; releasing the finalizer",
				fgKey.Name, mdb.Spec.DatabaseName)
			return ctrl.Result{}, r.removeFinalizer(ctx, mdb)
		}
		return ctrl.Result{}, fmt.Errorf("re-get failover group after drop: %w", err)
	}
	if refreshed.Status.ActiveSite != fg.Status.ActiveSite {
		r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseDropDeferred",
			"group %q's active site moved from %s to %s during the DROP; re-verifying before releasing the finalizer",
			fg.Name, fg.Status.ActiveSite, refreshed.Status.ActiveSite)
		return ctrl.Result{RequeueAfter: pendingRequeueDelay()}, nil
	}

	switch {
	case dropDB && len(dropOwners) > 0:
		r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "DatabaseDropped",
			"deletionPolicy=Delete: dropped database %q and user(s) %q on site %s",
			mdb.Spec.DatabaseName, strings.Join(principalNames(dropOwners), ", "), fg.Status.ActiveSite)
	case dropDB:
		r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "DatabaseDropped",
			"deletionPolicy=Delete: dropped database %q on site %s (owner user left untouched)",
			mdb.Spec.DatabaseName, fg.Status.ActiveSite)
	default:
		r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "DatabaseDropped",
			"deletionPolicy=Delete: dropped user(s) %q on site %s (database left untouched)",
			strings.Join(principalNames(dropOwners), ", "), fg.Status.ActiveSite)
	}
	logger.Info("dropped tenant database", "database", mdb.Spec.DatabaseName, "group", fg.Name)

	return ctrl.Result{}, r.removeFinalizer(ctx, mdb)
}

// deleteScope decides which MySQL objects the deleting CR may remove: the
// database (unless another live CR on the same group still declares it), the
// owner user(s), and the spec.users[] principals recorded in the
// status.appliedUsers ledger — each unless the username is reserved, another
// live CR shares the Secret or the username, or a sibling CR still lists the
// user in spec.grants[]. It returns the usernames that may be dropped; the
// owner is plural on purpose: a rotation that crashed after creating the new
// account but before status caught up leaves the new username in the
// status.pendingOwnerUser write-ahead record, and Delete must clean up both
// or leak a privileged user. The Secret itself is deliberately NOT a source
// of drop candidates: it names whatever the caller wrote last, which after a
// refused rotation (PreExistingOwnerUser) is a foreign account.
// CRs that are themselves being deleted don't count as claims — of two CRs
// deleted together, the first to reconcile drops, and the second's
// statements are IF EXISTS no-ops.
func (r *MysqlDatabaseReconciler) deleteScope(ctx context.Context, mdb *v1alpha1.MysqlDatabase, reserved map[string]bool) (dropDB bool, dropOwners []managedPrincipal, err error) {
	dropDB = true

	// Candidate owner usernames: the recorded one and the rotation
	// write-ahead record. Both are stamped before the SQL that creates the
	// account they name, so together they cover every owner account this
	// CR can have created; nothing else is consulted. Each is dropped on
	// exactly the hosts recorded for that name — not the spec's current
	// hosts, because a host no write-ahead recorded was never created by
	// this CR, and the two names' host lists can differ mid-rotation.
	candidates := []managedPrincipal{}
	if mdb.Status.OwnerUser != "" {
		candidates = append(candidates, managedPrincipal{username: mdb.Status.OwnerUser, hosts: recordedOwnerHosts(&mdb.Status)})
	}
	// The rotation write-ahead record names an account that may exist in
	// MySQL even though status.ownerUser never advanced to it.
	if p := mdb.Status.PendingOwnerUser; p != "" {
		if p == mdb.Status.OwnerUser {
			candidates[0].hosts = unionHosts(candidates[0].hosts, pendingOwnerHosts(&mdb.Status))
		} else {
			candidates = append(candidates, managedPrincipal{username: p, hosts: pendingOwnerHosts(&mdb.Status)})
		}
	}

	var list v1alpha1.MysqlDatabaseList
	if err := r.List(ctx, &list, client.InNamespace(mdb.Namespace)); err != nil {
		return false, nil, fmt.Errorf("list mysqldatabases for delete guard: %w", err)
	}
	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == mdb.Name || !other.DeletionTimestamp.IsZero() ||
			other.Spec.GroupRef.Name != mdb.Spec.GroupRef.Name {
			continue
		}
		if other.Spec.DatabaseName == mdb.Spec.DatabaseName {
			dropDB = false
		}
	}
	if !dropDB {
		r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseDropSkipped",
			"another MysqlDatabase still declares database %q; not dropping it", mdb.Spec.DatabaseName)
	}

	for _, c := range candidates {
		candidate := c.username
		if reserved[candidate] {
			r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "OwnerUserReservedSkipped",
				"owner user %q is a group-level principal of %q; not dropping it", candidate, mdb.Spec.GroupRef.Name)
			continue
		}
		if r.claimedBySibling(ctx, mdb, &list, candidate) {
			continue // claimedBySibling emitted the specific event.
		}
		if referrer, claimed := siblingGrantsClaimIn(&list, mdb.Name, mdb.Spec.GroupRef.Name, candidate); claimed {
			r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "OwnerUserDropSkipped",
				"owner user %q is listed in MysqlDatabase %q's spec.grants[]; not dropping it", candidate, referrer)
			continue
		}
		// The users[]-ledger direction (and a sibling's pending owner),
		// which claimedBySibling and the grants check don't cover.
		if referrer, how, claimed := siblingPrincipalClaimIn(&list, mdb.Name, mdb.Spec.GroupRef.Name, candidate); claimed {
			r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "OwnerUserDropSkipped",
				"owner user %q is still claimed by MysqlDatabase %q (%s); not dropping it", candidate, referrer, how)
			continue
		}
		dropOwners = append(dropOwners, c)
	}

	// users[] ledger candidates: every account a users[] entry recorded —
	// including a pending rotation target that never became current —
	// vetted through the same reserved and sibling-claim gates. The ledger
	// is the authority; the entries' Secrets may be long gone. A username
	// already on the list (the owner's, or another ledger record's) has
	// this record's hosts merged into it: the accounts differ by host, and
	// a drop that only covered the first candidate's hosts would orphan
	// the rest.
	dropped := make(map[string]int, len(dropOwners))
	for i, c := range dropOwners {
		dropped[c.username] = i
	}
	for _, state := range mdb.Status.AppliedUsers {
		for _, name := range ledgerUsernames(state) {
			if i, ok := dropped[name]; ok {
				dropOwners[i].hosts = unionHosts(dropOwners[i].hosts, ledgerHostsForName(state, name))
				continue
			}
			if reserved[name] {
				r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "UserReservedSkipped",
					"users[] principal %q is a group-level principal of %q; not dropping it", name, mdb.Spec.GroupRef.Name)
				continue
			}
			if referrer, how, claimed := siblingPrincipalClaimIn(&list, mdb.Name, mdb.Spec.GroupRef.Name, name); claimed {
				r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "UserDropSkipped",
					"users[] principal %q is still claimed by MysqlDatabase %q (%s); not dropping it", name, referrer, how)
				continue
			}
			dropped[name] = len(dropOwners)
			dropOwners = append(dropOwners, managedPrincipal{username: name, hosts: ledgerHostsForName(state, name)})
		}
	}
	return dropDB, dropOwners, nil
}

// claimedBySibling reports whether another live CR on the same group claims
// username as its owner — by sharing this CR's Secret, by recording the same
// username, or by naming a Secret that resolves to it. It emits the
// corresponding DatabaseDropSkipped event.
func (r *MysqlDatabaseReconciler) claimedBySibling(ctx context.Context, mdb *v1alpha1.MysqlDatabase, list *v1alpha1.MysqlDatabaseList, username string) bool {
	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == mdb.Name || !other.DeletionTimestamp.IsZero() ||
			other.Spec.GroupRef.Name != mdb.Spec.GroupRef.Name {
			continue
		}
		if other.Spec.Owner.SecretName == mdb.Spec.Owner.SecretName || other.Status.OwnerUser == username {
			r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseDropSkipped",
				"MysqlDatabase %q still claims owner user %q; not dropping it", other.Name, username)
			return true
		}
		if otherUser, err := r.ownerUsernameFromSecret(ctx, other); err == nil && otherUser == username {
			r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseDropSkipped",
				"MysqlDatabase %q's owner Secret still resolves to user %q; not dropping it", other.Name, username)
			return true
		}
	}
	return false
}

// siblingGrantsClaimIn reports whether any live CR on groupName — other than
// selfName, and excluding CRs being deleted — lists username in
// spec.grants[], returning the first referrer's name.
func siblingGrantsClaimIn(list *v1alpha1.MysqlDatabaseList, selfName, groupName, username string) (string, bool) {
	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == selfName || !other.DeletionTimestamp.IsZero() ||
			other.Spec.GroupRef.Name != groupName {
			continue
		}
		for _, g := range other.Spec.Grants {
			if g.Username == username {
				return other.Name, true
			}
		}
	}
	return "", false
}

// ownerUsernameFromSecret resolves the username a MysqlDatabase's owner
// Secret currently names. NotFound and read errors are returned to the
// caller — arbitration treats them differently than a resolved name.
func (r *MysqlDatabaseReconciler) ownerUsernameFromSecret(ctx context.Context, mdb *v1alpha1.MysqlDatabase) (string, error) {
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: mdb.Namespace, Name: mdb.Spec.Owner.SecretName}, &secret); err != nil {
		return "", err
	}
	return string(secret.Data["username"]), nil
}

// mdbOutranks reports whether a wins an ownership conflict against b: the
// older CR wins, name as the deterministic tie-break. Both sides compute the
// same answer, so exactly one CR of a conflicting pair goes Failed.
func mdbOutranks(a, b *v1alpha1.MysqlDatabase) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.Name < b.Name
}

// ownershipConflict verdicts: "" is no conflict; fail is a terminal
// conflict the CR must report; pending is an arbitration that cannot
// complete yet (a higher-ranked peer whose Secret is absent) and must wait
// rather than guess.
const (
	conflictVerdictFail    = "fail"
	conflictVerdictPending = "pending"
)

// ownershipConflict reports whether a higher-ranked live MysqlDatabase on
// the same group already claims this CR's database name or owner principal.
// A peer's owner username is taken from its status when recorded and from
// its Secret otherwise: status alone has a blind spot for peers that have
// not reconciled yet, and arbitration must not depend on whether the other
// CR happened to run first. When the peer's Secret cannot be read, the
// verdict is pending — failing open here would let two CRs share one MySQL
// account until the peer's first reconcile.
func (r *MysqlDatabaseReconciler) ownershipConflict(ctx context.Context, mdb *v1alpha1.MysqlDatabase, ownerUser string) (kind, reason, message string, err error) {
	var list v1alpha1.MysqlDatabaseList
	if err := r.List(ctx, &list, client.InNamespace(mdb.Namespace)); err != nil {
		return "", "", "", fmt.Errorf("list mysqldatabases for conflict check: %w", err)
	}
	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == mdb.Name || !other.DeletionTimestamp.IsZero() ||
			other.Spec.GroupRef.Name != mdb.Spec.GroupRef.Name {
			continue
		}
		if !mdbOutranks(other, mdb) {
			continue // we outrank; the other CR reports the conflict
		}
		if other.Spec.DatabaseName == mdb.Spec.DatabaseName {
			return conflictVerdictFail, "DatabaseNameConflict", fmt.Sprintf(
				"MysqlDatabase %q already declares database %q on group %q; two CRs must not manage one database (deletionPolicy: Delete on either would drop the other's data)",
				other.Name, mdb.Spec.DatabaseName, mdb.Spec.GroupRef.Name), nil
		}
		if other.Spec.Owner.SecretName == mdb.Spec.Owner.SecretName {
			return conflictVerdictFail, "OwnerConflict", fmt.Sprintf(
				"MysqlDatabase %q already shares owner Secret %q on group %q; each MysqlDatabase must own a distinct user (deleting one CR would drop the other's credential)",
				other.Name, mdb.Spec.Owner.SecretName, mdb.Spec.GroupRef.Name), nil
		}
		peerUser := other.Status.OwnerUser
		if peerUser == "" {
			peerUser, err = r.ownerUsernameFromSecret(ctx, other)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return conflictVerdictPending, "PeerOwnerSecretMissing", fmt.Sprintf(
						"higher-ranked MysqlDatabase %q's owner Secret %q cannot be read; deferring until the ownership check against it can be complete",
						other.Name, other.Spec.Owner.SecretName), nil
				}
				return "", "", "", fmt.Errorf("read owner secret of MysqlDatabase %q: %w", other.Name, err)
			}
		}
		if peerUser == ownerUser {
			return conflictVerdictFail, "OwnerConflict", fmt.Sprintf(
				"MysqlDatabase %q already owns MySQL user %q on group %q; each MysqlDatabase must own a distinct user (deleting one CR would drop the other's credential)",
				other.Name, ownerUser, mdb.Spec.GroupRef.Name), nil
		}
	}
	return "", "", "", nil
}

// findAppliedUser returns a pointer into states for secretName, or nil.
func findAppliedUser(states []v1alpha1.MysqlDatabaseUserState, secretName string) *v1alpha1.MysqlDatabaseUserState {
	for i := range states {
		if states[i].SecretName == secretName {
			return &states[i]
		}
	}
	return nil
}

// findAppliedUserIn is findAppliedUser over a value slice (the prior
// snapshot), returning a copy so callers cannot mutate the snapshot.
func findAppliedUserIn(states []v1alpha1.MysqlDatabaseUserState, secretName string) *v1alpha1.MysqlDatabaseUserState {
	for i := range states {
		if states[i].SecretName == secretName {
			s := *states[i].DeepCopy()
			return &s
		}
	}
	return nil
}

// replacedPendingHosts returns, per username, the hosts of pending records
// the write-ahead stamp is about to overwrite. A pending record survives
// dropStaleRotationTargets only when a current surface of this CR declares
// its name, so the receiving surface's stamp unions these hosts in: the
// accounts stay on record — under their new surface — until the post-apply
// cleanup has dropped the hosts that surface does not declare.
func replacedPendingHosts(st *v1alpha1.MysqlDatabaseStatus, ownerUser string, users []tenantUserInput) map[string][]string {
	out := map[string][]string{}
	if p := st.PendingOwnerUser; p != "" && st.OwnerUser != "" && st.OwnerUser != ownerUser && p != ownerUser {
		out[p] = unionHosts(out[p], pendingOwnerHosts(st))
	}
	for _, u := range users {
		e := findAppliedUser(st.AppliedUsers, u.entry.SecretName)
		if e == nil {
			continue
		}
		if p := e.PendingUsername; p != "" && e.Username != "" && e.Username != u.username && p != u.username {
			out[p] = unionHosts(out[p], ledgerPendingHosts(*e))
		}
	}
	return out
}

// stampOwnerWriteAhead records the owner account(s) this reconcile is about
// to create or re-apply. Hosts are tracked per recorded name: a first apply
// or a re-apply of the recorded owner grows OwnerHosts; a rotation records
// the target in PendingOwnerUser with its own PendingOwnerHosts, grown when
// retrying the same target and replaced when the target changed (the
// previous target was dropped, forgotten, or carried to the surface that now
// declares it). Unions run over the legacy-defaulted helper values so a
// pre-hosts record keeps its implied '%'.
func stampOwnerWriteAhead(st *v1alpha1.MysqlDatabaseStatus, ownerUser string, ownerHosts []string, carried map[string][]string) {
	switch {
	case st.OwnerUser == "":
		st.OwnerUser = ownerUser
		st.OwnerHosts = unionHosts(unionHosts(nil, ownerHosts), carried[ownerUser])
	case st.OwnerUser == ownerUser:
		st.OwnerHosts = unionHosts(unionHosts(recordedOwnerHosts(st), ownerHosts), carried[ownerUser])
	case st.PendingOwnerUser == ownerUser:
		st.PendingOwnerHosts = unionHosts(unionHosts(pendingOwnerHosts(st), ownerHosts), carried[ownerUser])
	default:
		st.PendingOwnerUser = ownerUser
		st.PendingOwnerHosts = unionHosts(unionHosts(nil, ownerHosts), carried[ownerUser])
	}
}

// stampUsersWriteAhead is stampOwnerWriteAhead for the users[] ledger: a
// missing entry is appended with its username (the first-apply write-ahead),
// and an entry whose Secret now names a different account records it as
// pendingUsername with its own pendingHosts (the rotation write-ahead).
func stampUsersWriteAhead(st *v1alpha1.MysqlDatabaseStatus, users []tenantUserInput, carried map[string][]string) {
	for _, u := range users {
		e := findAppliedUser(st.AppliedUsers, u.entry.SecretName)
		if e == nil {
			st.AppliedUsers = append(st.AppliedUsers, v1alpha1.MysqlDatabaseUserState{
				SecretName: u.entry.SecretName,
				Username:   u.username,
				Hosts:      unionHosts(unionHosts(nil, u.hosts), carried[u.username]),
			})
			continue
		}
		switch {
		case e.Username == "":
			e.Username = u.username
			e.Hosts = unionHosts(unionHosts(e.Hosts, u.hosts), carried[u.username])
		case e.Username == u.username:
			e.Hosts = unionHosts(unionHosts(ledgerHosts(*e), u.hosts), carried[u.username])
		case e.PendingUsername == u.username:
			e.PendingHosts = unionHosts(unionHosts(ledgerPendingHosts(*e), u.hosts), carried[u.username])
		default:
			e.PendingUsername = u.username
			e.PendingHosts = unionHosts(unionHosts(nil, u.hosts), carried[u.username])
		}
	}
}

// applyProgress records which principals had at least one statement
// execute (or possibly execute) during applyDatabase.
type applyProgress struct {
	schema bool
	owner  bool
	users  map[string]bool // by secretName
}

// statementExecuted reports whether a statement that returned err may have
// taken effect. A *mysql.MySQLError is the server's verdict on the
// statement, and MySQL 8 account and schema DDL is atomic, so it did not run;
// a connection, timeout or driver error is ambiguous and must be assumed to
// have run.
func statementExecuted(err error) bool {
	if err == nil {
		return true
	}
	var myErr *mysqldriver.MySQLError
	return !errors.As(err, &myErr)
}

// withdrawUnexecuted narrows the write-ahead records of every principal none
// of whose statements executed to the accounts the pre-reconcile snapshot
// already attributed to this CR. What remains after the narrowing is exactly
// the prior record plus any record carried over from another surface of this
// CR; every user@host that was only verified absent — and never created — is
// withdrawn, so an account someone else creates later is refused rather than
// adopted. Principals that did execute keep their stamp: their accounts may
// exist.
func withdrawUnexecuted(st *v1alpha1.MysqlDatabaseStatus, prior *v1alpha1.MysqlDatabaseStatus, progress applyProgress, users []tenantUserInput) {
	if !progress.schema {
		st.DatabaseCreated = prior.DatabaseCreated
	}
	if !progress.owner {
		if hosts := attributedHosts(prior, st.OwnerUser, recordedOwnerHosts(st)); len(hosts) > 0 {
			st.OwnerHosts = hosts
		} else {
			st.OwnerUser, st.OwnerHosts = "", nil
		}
		if hosts := attributedHosts(prior, st.PendingOwnerUser, pendingOwnerHosts(st)); len(hosts) > 0 {
			st.PendingOwnerHosts = hosts
		} else {
			st.PendingOwnerUser, st.PendingOwnerHosts = "", nil
		}
	}
	for _, u := range users {
		if progress.users[u.entry.SecretName] {
			continue
		}
		e := findAppliedUser(st.AppliedUsers, u.entry.SecretName)
		if e == nil {
			continue
		}
		if hosts := attributedHosts(prior, e.Username, ledgerHosts(*e)); len(hosts) > 0 {
			e.Hosts = hosts
		} else {
			e.Username, e.Hosts = "", nil
		}
		if hosts := attributedHosts(prior, e.PendingUsername, ledgerPendingHosts(*e)); len(hosts) > 0 {
			e.PendingHosts = hosts
		} else {
			e.PendingUsername, e.PendingHosts = "", nil
		}
		if e.Username == "" && e.PendingUsername == "" {
			kept := st.AppliedUsers[:0]
			for _, other := range st.AppliedUsers {
				if other.SecretName != u.entry.SecretName {
					kept = append(kept, other)
				}
			}
			st.AppliedUsers = kept
		}
	}
}

// attributedHosts filters hosts down to those on which prior attributes
// username to this CR.
func attributedHosts(prior *v1alpha1.MysqlDatabaseStatus, username string, hosts []string) []string {
	if username == "" {
		return nil
	}
	own := ownHostsFor(prior, username)
	var out []string
	for _, h := range hosts {
		if slices.Contains(own, h) {
			out = append(out, h)
		}
	}
	return out
}

// ownHostsFor is every host on which any of this CR's records names
// username: the recorded owner, the owner rotation target, and every users[]
// ledger entry's username and rotation target, each with its own hosts.
//
// This is the adoption memory, and it is deliberately per user@host and
// CR-wide. Per host, because a record for username@H1 says nothing about a
// same-named account on H2 that someone else created. CR-wide rather than
// per surface, because the records answer "did this CR create that
// account": a secretName rename, or a name moving between the owner and a
// users[] entry, must transfer the account rather than wedge on a refusal
// for an account this CR itself created.
func ownHostsFor(st *v1alpha1.MysqlDatabaseStatus, username string) []string {
	if username == "" {
		return nil
	}
	var out []string
	if st.OwnerUser == username {
		out = unionHosts(out, recordedOwnerHosts(st))
	}
	if st.PendingOwnerUser == username {
		out = unionHosts(out, pendingOwnerHosts(st))
	}
	for _, e := range st.AppliedUsers {
		out = unionHosts(out, ledgerHostsForName(e, username))
	}
	return out
}

// ownAccountAttributed reports whether the pre-reconcile records attribute
// the exact account username@host to this CR.
func ownAccountAttributed(prior *v1alpha1.MysqlDatabaseStatus, username, host string) bool {
	return slices.Contains(ownHostsFor(prior, username), host)
}

// ownNameRecorded reports whether any of this CR's records names username on
// any host. Only the users[] sibling-claim guard uses this username-level
// view: it arbitrates who may claim a name, not whether an account may be
// adopted.
func ownNameRecorded(prior *v1alpha1.MysqlDatabaseStatus, username string) bool {
	return len(ownHostsFor(prior, username)) > 0
}

// principalClaim is one surface of the current spec declaring a username.
type principalClaim struct {
	// surface names the declaring surface for events and logs.
	surface string
	// managed is true for the owner and users[] entries, whose accounts
	// this CR creates on hosts; false for grants[], which only grants onto
	// a '%' account created elsewhere.
	managed bool
	hosts   []string
}

// currentPrincipalClaims maps every username the current spec declares —
// owner, grants[] and resolved users[] — to its surface. Validate guarantees
// the surfaces do not overlap. Every drop path consults it, so a name that
// has moved between surfaces or entries is never dropped as "previous",
// "stale" or "removed" state: it is current desired state elsewhere.
func currentPrincipalClaims(ownerUser string, ownerHosts []string, grants []v1alpha1.MysqlDatabaseGrant, users []tenantUserInput) map[string]principalClaim {
	out := make(map[string]principalClaim, len(grants)+len(users)+1)
	out[ownerUser] = principalClaim{surface: "spec.owner", managed: true, hosts: ownerHosts}
	for _, g := range grants {
		out[g.Username] = principalClaim{surface: "spec.grants[]", hosts: defaultHosts}
	}
	for _, u := range users {
		out[u.username] = principalClaim{surface: fmt.Sprintf("spec.users[] entry %q", u.entry.SecretName), managed: true, hosts: u.hosts}
	}
	return out
}

// specHasUserSecret reports whether spec.users[] still declares secretName.
func specHasUserSecret(spec *v1alpha1.MysqlDatabaseSpec, secretName string) bool {
	for _, u := range spec.Users {
		if u.SecretName == secretName {
			return true
		}
	}
	return false
}

// ledgerUsernames returns the distinct non-empty account names a ledger
// entry may have created: the recorded username plus a pending rotation
// target that never became current.
func ledgerUsernames(state v1alpha1.MysqlDatabaseUserState) []string {
	var out []string
	if state.Username != "" {
		out = append(out, state.Username)
	}
	if state.PendingUsername != "" && state.PendingUsername != state.Username {
		out = append(out, state.PendingUsername)
	}
	return out
}

// readyAppliedUsers is the ledger a successful apply leaves behind: exactly
// the current spec entries, pendings resolved.
func readyAppliedUsers(users []tenantUserInput) []v1alpha1.MysqlDatabaseUserState {
	if len(users) == 0 {
		return nil
	}
	out := make([]v1alpha1.MysqlDatabaseUserState, len(users))
	for i, u := range users {
		out[i] = v1alpha1.MysqlDatabaseUserState{SecretName: u.entry.SecretName, Username: u.username, Hosts: unionHosts(nil, u.hosts)}
	}
	return out
}

// dropStaleRotationTargets drops rotation targets the records still hold as
// pending but the Secrets no longer name — BEFORE this reconcile's
// write-ahead overwrites those records. A rotation A→B whose drop of A
// failed transiently leaves {username: A, pending: B}; if the Secret then
// moves on to C, stamping pending=C first would make B unnameable: an
// account with tenant privileges that no later reconcile (or Delete) can
// find. Dropping B here — on exactly the hosts recorded for B, vetted like
// any other drop — keeps the invariant that every account this CR created
// stays on record until it is gone. A stale target that another current
// owner or users[] surface of this CR declares is left alone and keeps its
// record: the write-ahead re-records it under that surface in the same
// patch. One that became reserved, that a sibling claims, or that this CR's
// own grants[] now names is vetoed with the usual event and forgotten.
// Every clear of a pending name clears its pending hosts too. done reports
// that the reconcile must end with res/err.
func (r *MysqlDatabaseReconciler) dropStaleRotationTargets(ctx, sqlCtx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase, fg *v1alpha1.MysqlFailoverGroup,
	reserved map[string]bool, claims map[string]principalClaim, ownerUser string, users []tenantUserInput, prior *v1alpha1.MysqlDatabaseStatus) (res ctrl.Result, done bool, err error) {
	logger := log.FromContext(ctx)

	if stale := prior.PendingOwnerUser; stale != "" && stale != prior.OwnerUser && stale != ownerUser {
		if c, ok := claims[stale]; ok && c.managed {
			logger.V(1).Info("abandoned owner rotation target is now declared by this MysqlDatabase; not dropping",
				"pendingOwner", stale, "claimedBy", c.surface)
		} else {
			if res, done, err := r.retirePrincipal(ctx, sqlCtx, db, mdb, fg, reserved, claims, retirement{
				kind:     principalKindOwner,
				username: stale,
				hosts:    pendingOwnerHosts(prior),
				why:      fmt.Sprintf("as the abandoned owner rotation target before rotating to %q", ownerUser),
				what:     "abandoned owner rotation target",
				dropped: func() {
					r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "OwnerUserRotated",
						"dropped abandoned owner rotation target %q before rotating to %q", stale, ownerUser)
				},
			}); done {
				return res, true, err
			}
			if serr := r.stampStatus(ctx, mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
				if st.PendingOwnerUser == stale {
					st.PendingOwnerUser, st.PendingOwnerHosts = "", nil
				}
			}); serr != nil {
				return ctrl.Result{}, true, serr
			}
			prior.PendingOwnerUser, prior.PendingOwnerHosts = "", nil
		}
	}

	for _, u := range users {
		prev := findAppliedUserIn(prior.AppliedUsers, u.entry.SecretName)
		if prev == nil {
			continue
		}
		stale := prev.PendingUsername
		if stale == "" || stale == prev.Username || stale == u.username {
			continue
		}
		if c, ok := claims[stale]; ok && c.managed {
			continue
		}
		secretName, current := u.entry.SecretName, u.username
		if res, done, err := r.retirePrincipal(ctx, sqlCtx, db, mdb, fg, reserved, claims, retirement{
			kind:     principalKindUser,
			username: stale,
			hosts:    ledgerPendingHosts(*prev),
			why:      fmt.Sprintf("as the abandoned rotation target of spec.users[] entry %q", secretName),
			what:     "abandoned users[] rotation target",
			dropped: func() {
				r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "UserRotated",
					"dropped abandoned rotation target %q of users[] entry %q before rotating to %q", stale, secretName, current)
			},
		}); done {
			return res, true, err
		}
		if serr := r.stampStatus(ctx, mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
			if e := findAppliedUser(st.AppliedUsers, secretName); e != nil && e.PendingUsername == stale {
				e.PendingUsername, e.PendingHosts = "", nil
			}
		}); serr != nil {
			return ctrl.Result{}, true, serr
		}
		if e := findAppliedUser(prior.AppliedUsers, secretName); e != nil {
			e.PendingUsername, e.PendingHosts = "", nil
		}
	}
	return ctrl.Result{}, false, nil
}

// principalKind selects the event reasons a retirement's vetoes emit.
type principalKind int

const (
	principalKindOwner principalKind = iota
	principalKindUser
)

// retirement describes one no-longer-desired account name of this CR to
// revoke and drop on exactly the hosts recorded for that name.
type retirement struct {
	kind     principalKind
	username string
	hosts    []string
	// revoke strips the name's rights on this database first; the revoke
	// runs even when the drop is vetoed.
	revoke bool
	// why suffixes veto events; what names the account in errors.
	why  string
	what string
	// dropped emits the success event.
	dropped func()
}

// retirePrincipal revokes (optionally) and drops a retired account name of
// this CR. The caller has already excluded names another owner or users[]
// surface of this CR declares. The name's '%' account is excluded when this
// CR's own spec.grants[] declares it (grants[] manages exactly that account);
// the drop is vetoed when the name is now a group-level principal or a live
// sibling claims it. done reports that the reconcile must end with res/err.
func (r *MysqlDatabaseReconciler) retirePrincipal(ctx, sqlCtx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase, fg *v1alpha1.MysqlFailoverGroup,
	reserved map[string]bool, claims map[string]principalClaim, rt retirement) (ctrl.Result, bool, error) {
	logger := log.FromContext(ctx)
	end := func(res ctrl.Result, err error) (ctrl.Result, bool, error) { return res, true, err }

	noun, skipReason, reservedReason := "owner user", "OwnerUserDropSkipped", "OwnerUserReservedSkipped"
	if rt.kind == principalKindUser {
		noun, skipReason, reservedReason = "users[] principal", "UserDropSkipped", "UserReservedSkipped"
	}

	hosts := rt.hosts
	if c, ok := claims[rt.username]; ok {
		if c.managed {
			return ctrl.Result{}, false, nil
		}
		hosts = withoutHost(hosts, tenantUserHost)
		if len(hosts) == 0 {
			r.Recorder.Eventf(mdb, corev1.EventTypeWarning, skipReason,
				"%s %q is declared in this MysqlDatabase's %s; not dropping it %s", noun, rt.username, c.surface, rt.why)
			return ctrl.Result{}, false, nil
		}
	}
	if len(hosts) == 0 {
		return ctrl.Result{}, false, nil
	}

	if rt.revoke {
		revokeStmt, rerr := renderRevokeAll("status record username", mdb.Spec.DatabaseName, rt.username, hosts)
		if rerr != nil {
			// A status value that no longer renders is unreachable via any
			// input we accept; log and move on rather than wedging the CR.
			logger.Error(rerr, "cannot render revoke for retired principal; skipping", "username", rt.username)
			return ctrl.Result{}, false, nil
		}
		if _, xerr := db.ExecContext(sqlCtx, revokeStmt); xerr != nil {
			if transientSQLError(xerr) {
				return end(r.pending(ctx, mdb, "PrimaryUnavailable",
					fmt.Sprintf("transient MySQL error revoking %s on group %q: %v", rt.what, fg.Name, xerr)))
			}
			return end(r.fail(ctx, mdb, "MySQLError", fmt.Sprintf("revoke %s %q: %v", rt.what, rt.username, xerr)))
		}
	}

	if reserved[rt.username] {
		r.Recorder.Eventf(mdb, corev1.EventTypeWarning, reservedReason,
			"%s %q is a group-level principal of %q; not dropping it %s", noun, rt.username, fg.Name, rt.why)
		return ctrl.Result{}, false, nil
	}
	var list v1alpha1.MysqlDatabaseList
	if err := r.List(ctx, &list, client.InNamespace(mdb.Namespace)); err != nil {
		return end(ctrl.Result{}, fmt.Errorf("list mysqldatabases for drop guard: %w", err))
	}
	if referrer, how, claimed := siblingPrincipalClaimIn(&list, mdb.Name, mdb.Spec.GroupRef.Name, rt.username); claimed {
		r.Recorder.Eventf(mdb, corev1.EventTypeWarning, skipReason,
			"%s %q is still claimed by MysqlDatabase %q (%s); not dropping it %s", noun, rt.username, referrer, how, rt.why)
		return ctrl.Result{}, false, nil
	}

	dropStmt, derr := renderDropUser("status record username", rt.username, hosts)
	if derr != nil {
		logger.Error(derr, "cannot render drop for retired principal; skipping", "username", rt.username)
		return ctrl.Result{}, false, nil
	}
	if _, xerr := db.ExecContext(sqlCtx, dropStmt); xerr != nil {
		if transientSQLError(xerr) {
			return end(r.pending(ctx, mdb, "PrimaryUnavailable",
				fmt.Sprintf("transient MySQL error dropping %s on group %q: %v", rt.what, fg.Name, xerr)))
		}
		return end(r.fail(ctx, mdb, "MySQLError", fmt.Sprintf("drop %s %q: %v", rt.what, rt.username, xerr)))
	}
	rt.dropped()
	return ctrl.Result{}, false, nil
}

// dropUndeclaredHosts drops a current owner or users[] username on every host
// the pre-reconcile records attribute to it but the spec no longer declares.
func (r *MysqlDatabaseReconciler) dropUndeclaredHosts(ctx, sqlCtx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase, fg *v1alpha1.MysqlFailoverGroup,
	kind, username string, desired []string, prior *v1alpha1.MysqlDatabaseStatus, dropped func(removed []string)) (ctrl.Result, bool, error) {
	removed := diffHosts(ownHostsFor(prior, username), desired)
	if len(removed) == 0 {
		return ctrl.Result{}, false, nil
	}
	dropStmt, derr := renderDropUser(kind, username, removed)
	if derr != nil {
		log.FromContext(ctx).Error(derr, "cannot render drop for removed hosts; skipping", "username", username, "hosts", removed)
		return ctrl.Result{}, false, nil
	}
	if _, xerr := db.ExecContext(sqlCtx, dropStmt); xerr != nil {
		if transientSQLError(xerr) {
			res, err := r.pending(ctx, mdb, "PrimaryUnavailable",
				fmt.Sprintf("transient MySQL error dropping removed hosts of %q on group %q: %v", username, fg.Name, xerr))
			return res, true, err
		}
		res, err := r.fail(ctx, mdb, "MySQLError",
			fmt.Sprintf("drop %q on removed hosts %v: %v", username, removed, xerr))
		return res, true, err
	}
	dropped(removed)
	return ctrl.Result{}, false, nil
}

// siblingPrincipalClaimIn reports whether any live sibling CR on groupName
// claims username through any of its principal surfaces: recorded or pending
// owner, a spec.grants[] entry, or a users[] ledger record. It is the
// union-of-claims check every apply-path drop shares; the delete path's owner
// candidates additionally resolve sibling owner Secrets (claimedBySibling).
func siblingPrincipalClaimIn(list *v1alpha1.MysqlDatabaseList, selfName, groupName, username string) (referrer, how string, claimed bool) {
	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == selfName || !other.DeletionTimestamp.IsZero() ||
			other.Spec.GroupRef.Name != groupName {
			continue
		}
		if other.Status.OwnerUser == username || other.Status.PendingOwnerUser == username {
			return other.Name, "owner", true
		}
		for _, g := range other.Spec.Grants {
			if g.Username == username {
				return other.Name, "spec.grants[]", true
			}
		}
		for _, s := range other.Status.AppliedUsers {
			if s.Username == username || s.PendingUsername == username {
				return other.Name, "status.appliedUsers", true
			}
		}
	}
	return "", "", false
}

// preflightAdoption runs every adoption gate — read-only queries only —
// before anything is written ahead: the schema, and every owner and users[]
// user@host. An existing object is this CR's to manage only when the
// pre-reconcile records attribute it: status.databaseCreated for the schema,
// and a record naming that exact user on that exact host for an account.
// Stopping at the first refusal is safe because nothing has been stamped.
// It returns whether the schema exists, so the apply does not re-query.
func preflightAdoption(ctx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase, ownerUser string, ownerHosts []string, users []tenantUserInput, prior *v1alpha1.MysqlDatabaseStatus) (bool, error) {
	dbExists, err := schemaExists(ctx, db, mdb.Spec.DatabaseName)
	if err != nil {
		return false, fmt.Errorf("check schema existence: %w", err)
	}
	if dbExists && !prior.DatabaseCreated {
		return false, &errDatabasePreExists{database: mdb.Spec.DatabaseName}
	}
	for _, h := range ownerHosts {
		exists, err := mysqlAccountExists(ctx, db, ownerUser, h)
		if err != nil {
			return false, fmt.Errorf("check owner user existence: %w", err)
		}
		if exists && !ownAccountAttributed(prior, ownerUser, h) {
			return false, &errPreExistingOwnerUser{username: ownerUser, host: h}
		}
	}
	for i, u := range users {
		for _, h := range u.hosts {
			exists, err := mysqlAccountExists(ctx, db, u.username, h)
			if err != nil {
				return false, fmt.Errorf("check spec.users[%d] user: %w", i, err)
			}
			if exists && !ownAccountAttributed(prior, u.username, h) {
				return false, &errPreExistingUser{secretName: u.entry.SecretName, username: u.username, host: h}
			}
		}
	}
	return dbExists, nil
}

// applyDatabase runs the idempotent apply sequence on an open admin
// connection and returns the usernames granted, owner first, plus which
// principals had a statement execute (see withdrawUnexecuted).
//
// Every statement is rendered — and therefore validated — before any of them
// executes, so a bad identifier cannot produce a partially-applied tenant.
// The adoption gates ran in preflightAdoption, before the write-ahead stamp;
// the one deliberately interleaved check left is each spec.grants[] check
// immediately before that user's GRANT, which must abort rather than fall
// through to a CREATE USER.
//
// Privilege application is grant-then-revoke: the desired set is GRANTed
// first and only the surplus revoked afterwards, so a failure mid-sequence
// leaves the principal over-granted for one requeue interval rather than
// with zero privileges on its own live database.
func applyDatabase(ctx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase, ownerUser, ownerPass string, ownerHosts []string, users []tenantUserInput, prior *v1alpha1.MysqlDatabaseStatus, dbExists bool) ([]string, applyProgress, error) {
	spec := &mdb.Spec
	progress := applyProgress{users: make(map[string]bool, len(users))}

	createDB, err := renderCreateDatabase(spec.DatabaseName, spec.EffectiveCharacterSet(), spec.EffectiveCollation())
	if err != nil {
		return nil, progress, err
	}
	alterDB, err := renderAlterDatabase(spec.DatabaseName, spec.EffectiveCharacterSet(), spec.EffectiveCollation())
	if err != nil {
		return nil, progress, err
	}
	ownerStmts, err := renderOwnerUserStatements(ownerUser, ownerPass, ownerHosts)
	if err != nil {
		return nil, progress, err
	}
	ownerPrivs := spec.EffectiveOwnerPrivileges()
	ownerGrant, err := renderGrant("spec.owner.privileges", ownerPrivs, spec.DatabaseName, ownerUser, ownerHosts)
	if err != nil {
		return nil, progress, err
	}
	ownerSurplus, err := renderRevokeSurplus("spec.owner secret username", ownerPrivs, spec.DatabaseName, ownerUser, ownerHosts)
	if err != nil {
		return nil, progress, err
	}
	type renderedTenantUser struct {
		stmts   []string
		grant   string
		surplus string
	}
	renderedUsers := make([]renderedTenantUser, len(users))
	for i, u := range users {
		kind := fmt.Sprintf("spec.users[%d] secret username", i)
		stmts, err := renderTenantUserStatements(kind, u.username, u.password, u.hosts, u.entry.ResourceLimits)
		if err != nil {
			return nil, progress, err
		}
		grant, err := renderGrant(fmt.Sprintf("spec.users[%d].privileges", i), u.entry.Privileges, spec.DatabaseName, u.username, u.hosts)
		if err != nil {
			return nil, progress, err
		}
		surplus, err := renderRevokeSurplus(kind, u.entry.Privileges, spec.DatabaseName, u.username, u.hosts)
		if err != nil {
			return nil, progress, err
		}
		renderedUsers[i] = renderedTenantUser{stmts: stmts, grant: grant, surplus: surplus}
	}
	grantStmts := make([]string, len(spec.Grants))
	grantSurplus := make([]string, len(spec.Grants))
	for i, g := range spec.Grants {
		stmt, err := renderGrant(fmt.Sprintf("spec.grants[%d].privileges", i), g.Privileges, spec.DatabaseName, g.Username, defaultHosts)
		if err != nil {
			return nil, progress, err
		}
		grantStmts[i] = stmt
		surplus, err := renderRevokeSurplus(fmt.Sprintf("spec.grants[%d].username", i), g.Privileges, spec.DatabaseName, g.Username, defaultHosts)
		if err != nil {
			return nil, progress, err
		}
		grantSurplus[i] = surplus
	}
	// Entries removed from spec.grants[] since the last successful apply
	// get revoked: grants[] is desired state on the way out as well as on
	// the way in. The usernames come from status.appliedGrants, which is
	// exactly why that field exists.
	current := make(map[string]bool, len(spec.Grants)+len(users)+1)
	current[ownerUser] = true
	for _, g := range spec.Grants {
		current[g.Username] = true
	}
	// users[] usernames count as current too: a principal that moved from
	// grants[] to users[] between applies keeps its (users[]-managed)
	// privileges instead of being revoked as a removed grant.
	for _, u := range users {
		current[u.username] = true
	}
	var removedRevokes []string
	for _, user := range prior.AppliedGrants {
		// A name this CR's own owner or users[] records hold (the owner
		// rotated away, say) is not a removed grant: its retirement path
		// revokes it on its recorded hosts, after the same vetting as its
		// drop.
		if current[user] || ownNameRecorded(prior, user) {
			continue
		}
		stmt, err := renderRevokeAll("status.appliedGrants entry", spec.DatabaseName, user, defaultHosts)
		if err != nil {
			return nil, progress, err
		}
		removedRevokes = append(removedRevokes, stmt)
	}

	// exec runs one statement and marks its principal's step as executed
	// unless MySQL refused the statement outright.
	exec := func(executed *bool, stmt string) error {
		_, err := db.ExecContext(ctx, stmt)
		if executed != nil && statementExecuted(err) {
			*executed = true
		}
		if err != nil {
			return fmt.Errorf("exec %s: %w", credentialStatementErrorLabel(mdb.Name, stmt), err)
		}
		return nil
	}

	// The schema adoption gate ran in the preflight: an existing schema
	// reaching this point is this CR's (status.databaseCreated).
	if !dbExists {
		if err := exec(&progress.schema, createDB); err != nil {
			return nil, progress, err
		}
	}
	// characterSet/collation are mutable desired state; CREATE DATABASE IF
	// NOT EXISTS would never apply an edit to an existing schema, so the
	// ALTER runs on every apply. It only changes schema defaults.
	if err := exec(&progress.schema, alterDB); err != nil {
		return nil, progress, err
	}

	for _, stmt := range ownerStmts {
		if err := exec(&progress.owner, stmt); err != nil {
			return nil, progress, err
		}
	}
	if err := exec(&progress.owner, ownerGrant); err != nil {
		return nil, progress, err
	}
	if ownerSurplus != "" {
		if err := exec(&progress.owner, ownerSurplus); err != nil {
			return nil, progress, err
		}
	}

	for i, u := range users {
		var executed bool
		run := func(stmt string) error {
			err := exec(&executed, stmt)
			progress.users[u.entry.SecretName] = executed
			return err
		}
		for _, stmt := range renderedUsers[i].stmts {
			if err := run(stmt); err != nil {
				return nil, progress, err
			}
		}
		if err := run(renderedUsers[i].grant); err != nil {
			return nil, progress, err
		}
		if renderedUsers[i].surplus != "" {
			if err := run(renderedUsers[i].surplus); err != nil {
				return nil, progress, err
			}
		}
	}

	applied := []string{ownerUser}
	for i, g := range spec.Grants {
		exists, err := mysqlAccountExists(ctx, db, g.Username, tenantUserHost)
		if err != nil {
			return nil, progress, fmt.Errorf("check spec.grants[%d] user: %w", i, err)
		}
		if !exists {
			return nil, progress, &errGrantUserMissing{username: g.Username}
		}
		if err := exec(nil, grantStmts[i]); err != nil {
			return nil, progress, err
		}
		if grantSurplus[i] != "" {
			if err := exec(nil, grantSurplus[i]); err != nil {
				return nil, progress, err
			}
		}
		applied = append(applied, g.Username)
	}

	for _, stmt := range removedRevokes {
		if err := exec(nil, stmt); err != nil {
			return nil, progress, err
		}
	}

	return applied, progress, nil
}

// dropDatabase is the deletionPolicy: Delete path. It revokes before it
// drops, because MySQL leaves schema-level grant rows behind when a schema
// disappears, and it never drops a spec.grants[] user: those principals are
// shared and this CRD did not create them. dropDB and dropOwners come from
// deleteScope — the schema drop is suppressed when another live CR still
// declares the database, and each owner username is individually vetted.
func dropDatabase(ctx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase, dropDB bool, dropOwners []managedPrincipal) error {
	spec := &mdb.Spec

	stmts := make([]string, 0, len(spec.Grants)+len(mdb.Status.AppliedGrants)+len(dropOwners)+2)
	if dropDB {
		// Revoke the union of the grants currently declared and the ones
		// recorded by earlier applies: an entry removed from spec.grants[]
		// must not survive the delete as a lingering mysql.db row that
		// reactivates if the schema name is ever recreated.
		revoked := make(map[mysqlAccount]bool)
		revokeFor := func(kind, username string, hosts []string) error {
			var pending []string
			for _, h := range hosts {
				a := mysqlAccount{user: username, host: h}
				if revoked[a] {
					continue
				}
				revoked[a] = true
				pending = append(pending, h)
			}
			if len(pending) == 0 {
				return nil
			}
			stmt, err := renderRevokeAll(kind, spec.DatabaseName, username, pending)
			if err != nil {
				return err
			}
			stmts = append(stmts, stmt)
			return nil
		}
		for i, g := range spec.Grants {
			if err := revokeFor(fmt.Sprintf("spec.grants[%d].username", i), g.Username, defaultHosts); err != nil {
				return err
			}
		}
		// appliedGrants is owner-first; the owner's accounts live on the
		// hosts recorded for that name, grants[] principals on '%'.
		for _, username := range mdb.Status.AppliedGrants {
			hosts := defaultHosts
			if username == mdb.Status.OwnerUser {
				hosts = recordedOwnerHosts(&mdb.Status)
			}
			if err := revokeFor("status.appliedGrants entry", username, hosts); err != nil {
				return err
			}
		}
		// The owner rotation target and the users[] ledger principals lose
		// their rights on this database even when their drop was vetoed:
		// a sibling-claimed principal survives as an account, but its
		// mysql.db rows for this schema must not linger past the DROP and
		// reactivate on a recreated name.
		if p := mdb.Status.PendingOwnerUser; p != "" {
			if err := revokeFor("status.pendingOwnerUser", p, pendingOwnerHosts(&mdb.Status)); err != nil {
				return err
			}
		}
		for _, state := range mdb.Status.AppliedUsers {
			for _, username := range ledgerUsernames(state) {
				if err := revokeFor("status.appliedUsers entry", username, ledgerHostsForName(state, username)); err != nil {
					return err
				}
			}
		}
		dropDBStmt, err := renderDropDatabase(spec.DatabaseName)
		if err != nil {
			return err
		}
		stmts = append(stmts, dropDBStmt)
	}

	// The owner users are only dropped when deleteScope vetted them: the
	// apply path records status.ownerUser before the first statement runs,
	// so even a partially-applied owner is covered, and the pending record
	// covers a rotation that crashed mid-handover.
	for _, p := range dropOwners {
		dropUser, err := renderDropUser("managed principal username", p.username, p.hosts)
		if err != nil {
			return err
		}
		stmts = append(stmts, dropUser)
	}

	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %s: %w", credentialStatementErrorLabel(mdb.Name, stmt), err)
		}
	}
	return nil
}

// mysqlAccountExists answers the existence checks with a parameterized
// query — user and host are compared, never rendered.
func mysqlAccountExists(ctx context.Context, db *sql.DB, username, host string) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, grantUserExistsQuery, username, host).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// unionHosts returns a ∪ b preserving first-seen order.
func unionHosts(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	var out []string
	for _, list := range [][]string{a, b} {
		for _, h := range list {
			if h == "" || seen[h] {
				continue
			}
			seen[h] = true
			out = append(out, h)
		}
	}
	return out
}

// diffHosts returns the hosts in prior that are not in current.
func diffHosts(prior, current []string) []string {
	keep := make(map[string]bool, len(current))
	for _, h := range current {
		keep[h] = true
	}
	var out []string
	for _, h := range prior {
		if !keep[h] {
			out = append(out, h)
		}
	}
	return out
}

// withoutHost returns hosts minus host.
func withoutHost(hosts []string, host string) []string {
	return diffHosts(hosts, []string{host})
}

// recordedOwnerHosts is the hosts of status.ownerUser with the pre-hosts
// default applied: a CR that recorded an owner before the field existed
// created it on '%'. A CR with no recorded owner has no hosts.
func recordedOwnerHosts(st *v1alpha1.MysqlDatabaseStatus) []string {
	if st.OwnerUser == "" {
		return nil
	}
	if len(st.OwnerHosts) > 0 {
		return st.OwnerHosts
	}
	return defaultHosts
}

// pendingOwnerHosts is the hosts of status.pendingOwnerUser. An older
// operator kept one host union shared by both owner names and never wrote
// pendingOwnerHosts; that shape falls back to the shared record so an
// in-flight rotation straddling the upgrade still covers its accounts.
func pendingOwnerHosts(st *v1alpha1.MysqlDatabaseStatus) []string {
	if st.PendingOwnerUser == "" {
		return nil
	}
	if len(st.PendingOwnerHosts) > 0 {
		return st.PendingOwnerHosts
	}
	if len(st.OwnerHosts) > 0 {
		return st.OwnerHosts
	}
	return defaultHosts
}

// ledgerHosts is a users[] ledger entry's recorded-username hosts with the
// pre-hosts default applied.
func ledgerHosts(state v1alpha1.MysqlDatabaseUserState) []string {
	if len(state.Hosts) > 0 {
		return state.Hosts
	}
	return defaultHosts
}

// ledgerPendingHosts is pendingOwnerHosts for a users[] ledger entry: the
// hosts of its pendingUsername, falling back to the shared hosts record an
// older operator wrote.
func ledgerPendingHosts(state v1alpha1.MysqlDatabaseUserState) []string {
	if state.PendingUsername == "" {
		return nil
	}
	if len(state.PendingHosts) > 0 {
		return state.PendingHosts
	}
	return ledgerHosts(state)
}

// ledgerHostsForName is every host a ledger entry records for username,
// whether as its username, its rotation target, or both.
func ledgerHostsForName(state v1alpha1.MysqlDatabaseUserState, username string) []string {
	if username == "" {
		return nil
	}
	var out []string
	if state.Username == username {
		out = unionHosts(out, ledgerHosts(state))
	}
	if state.PendingUsername == username {
		out = unionHosts(out, ledgerPendingHosts(state))
	}
	return out
}

// schemaExists answers the adoption gate with a parameterized query — the
// schema name is compared, never rendered.
func schemaExists(ctx context.Context, db *sql.DB, database string) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, schemaExistsQuery, database).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// groupFenced reports whether the group's primary is currently fenced by an
// in-place restore or a planned failover, in which case a MysqlDatabase backs
// off to Pending instead of erroring.
//
// It delegates to inPlaceRestoreInFlight and plannedFailoverInFlight — the
// same classifiers the topology manager freezes on — rather than keeping a
// private copy of which phases count as active. A private copy would drift
// the first time a phase is added, and its disagreement with the canonical
// helpers would be exactly the window where a tenant runs DDL against a
// primary the operator considers fenced.
func groupFenced(fg *v1alpha1.MysqlFailoverGroup) (reason, message string, fenced bool) {
	if inPlaceRestoreInFlight(fg) {
		phase := "requested" // spec set, status not yet observed
		if fg.Status.RestoreInPlace != nil {
			phase = string(fg.Status.RestoreInPlace.Phase)
		}
		return "RestoreInProgress",
			fmt.Sprintf("group %q is mid in-place restore (phase %s)", fg.Name, phase), true
	}
	if plannedFailoverInFlight(fg.Status.PlannedFailover) {
		return "PlannedFailoverInProgress",
			fmt.Sprintf("group %q is mid planned failover (phase %s)", fg.Name, fg.Status.PlannedFailover.Phase), true
	}
	return "", "", false
}

// computeDatabaseHash fingerprints everything an apply depends on: the
// spec, the owner Secret's revision, the active site, and the group's
// identity.
//
// The Secret contributes its UID and resourceVersion, never a digest of
// its bytes: status is caller-readable without Secret access, and a
// content digest would let a status reader offline-check password guesses
// against the hash. A revision changes exactly when the bytes do, so the
// skip check loses nothing.
//
// The group's UID and its latest completed restore are included so a
// recreated group — or a same-site in-place restore whose fence transitions
// were missed while the operator was down — cannot leave a CR falsely Ready
// on an unchanged spec+Secret hash.
//
// Including the active site is what makes "re-run after failover" and
// "skip if unchanged" coexist: without it, the skip check would swallow the
// very re-apply the failover watch exists to trigger.
func computeDatabaseHash(mdb *v1alpha1.MysqlDatabase, ownerSecret *corev1.Secret, users []tenantUserInput, fg *v1alpha1.MysqlFailoverGroup) (string, error) {
	h := sha256.New()

	specJSON, err := json.Marshal(mdb.Spec)
	if err != nil {
		return "", fmt.Errorf("marshal spec: %w", err)
	}
	fmt.Fprintf(h, "spec=%s\n", specJSON)
	fmt.Fprintf(h, "activeSite=%s\n", fg.Status.ActiveSite)
	fmt.Fprintf(h, "secret=%s/%s\n", ownerSecret.UID, ownerSecret.ResourceVersion)
	// users[] Secrets contribute revisions under the same
	// no-content-digest rule as the owner Secret; iteration follows spec
	// order, which the list-map key keeps stable.
	for _, u := range users {
		fmt.Fprintf(h, "userSecret=%s=%s/%s\n", u.entry.SecretName, u.secret.UID, u.secret.ResourceVersion)
	}
	fmt.Fprintf(h, "group=%s\n", fg.UID)
	if fg.Status.RestoreInPlace != nil {
		fmt.Fprintf(h, "restore=%s\n", fg.Status.RestoreInPlace.ConfirmTokenUsed)
	}

	return hex.EncodeToString(h.Sum(nil))[:16], nil
}

func (r *MysqlDatabaseReconciler) dialer() openMySQLFunc {
	if r.OpenDB != nil {
		return r.OpenDB
	}
	return openMySQL
}

// pending records a dependency that is not ready yet and requeues. Pending is
// never an error: the CR is fine, the world is not ready.
func (r *MysqlDatabaseReconciler) pending(ctx context.Context, mdb *v1alpha1.MysqlDatabase, reason, message string) (ctrl.Result, error) {
	if err := r.stampStatus(ctx, mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
		st.Phase = v1alpha1.MysqlDatabasePhasePending
		st.ObservedGeneration = mdb.Generation
		st.Message = message
		setCondition(&st.Conditions, metav1.Condition{
			Type:               ConditionDatabaseReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: mdb.Generation,
			LastTransitionTime: metav1.Now(),
			Reason:             reason,
			Message:            message,
		})
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: pendingRequeueDelay()}, nil
}

// pendingRequeueDelay spreads Pending retries over [15s, 45s) instead of a
// fixed 30s. A failover re-enqueues every tenant on the group at once; a
// fixed interval would bring the whole wave back in lockstep, retrying
// against a primary that may still be read-only, over and over, on the same
// beat. Jitter costs at most half a requeue interval of convergence
// latency and removes the lockstep.
func pendingRequeueDelay() time.Duration {
	return mysqlDatabasePendingRequeue/2 + time.Duration(rand.Int64N(int64(mysqlDatabasePendingRequeue)))
}

// fail records a problem that will not resolve on its own. It returns no
// error so the CR does not hot-loop through exponential backoff while a human
// reads the message; the slow requeue still lets it self-heal if the fix
// happened outside Kubernetes (a missing grant user being created, say).
// Every failure reason emits a Warning event, so `kubectl describe` tells
// the same story for all of them rather than only the hand-picked few.
func (r *MysqlDatabaseReconciler) fail(ctx context.Context, mdb *v1alpha1.MysqlDatabase, reason, message string) (ctrl.Result, error) {
	r.Recorder.Event(mdb, corev1.EventTypeWarning, reason, message)
	if err := r.stampStatus(ctx, mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
		st.Phase = v1alpha1.MysqlDatabasePhaseFailed
		st.ObservedGeneration = mdb.Generation
		st.Message = message
		setCondition(&st.Conditions, metav1.Condition{
			Type:               ConditionDatabaseReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: mdb.Generation,
			LastTransitionTime: metav1.Now(),
			Reason:             reason,
			Message:            message,
		})
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: mysqlDatabaseFailedRequeue}, nil
}

func (r *MysqlDatabaseReconciler) stampStatus(ctx context.Context, mdb *v1alpha1.MysqlDatabase, mutate func(*v1alpha1.MysqlDatabaseStatus)) error {
	patch := client.MergeFrom(mdb.DeepCopy())
	before := mdb.Status.DeepCopy()
	mutate(&mdb.Status)
	// Parked CRs re-run pending()/fail() on every requeue; a status that
	// did not actually change must not cost a status-subresource write per
	// interval. setCondition preserves LastTransitionTime when the status
	// is unchanged, so a true no-op compares equal.
	if reflect.DeepEqual(*before, mdb.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, mdb, patch); err != nil {
		return fmt.Errorf("update mysqldatabase status: %w", err)
	}
	return nil
}

func (r *MysqlDatabaseReconciler) removeFinalizer(ctx context.Context, mdb *v1alpha1.MysqlDatabase) error {
	controllerutil.RemoveFinalizer(mdb, MysqlDatabaseFinalizer)
	if err := r.Update(ctx, mdb); err != nil {
		return fmt.Errorf("remove finalizer: %w", err)
	}
	return nil
}

// mdbSecretNames is the mdbSecretNamesIndex extractor: every Secret a CR
// references — the owner's and each users[] entry's. Shared with the tests'
// fake-client index so a change here cannot leave the fake indexing a stale
// field set while the mapping tests still pass.
func mdbSecretNames(o client.Object) []string {
	mdb := o.(*v1alpha1.MysqlDatabase)
	names := make([]string, 0, len(mdb.Spec.Users)+1)
	names = append(names, mdb.Spec.Owner.SecretName)
	for _, u := range mdb.Spec.Users {
		names = append(names, u.SecretName)
	}
	return names
}

// SetupWithManager registers the reconciler with the manager.
//
// The three Watches are not conveniences:
//
//   - The MysqlFailoverGroup watch is what makes a MysqlDatabase correct
//     across a failover. Grants replicate, but a CR must not report Ready
//     against a stale primary, so every matching CR is re-enqueued when the
//     group's active site (or its fenced state) changes.
//   - The Secret watch is what makes "rotation is a Secret write and nothing
//     else" true. Without it, a rotated password would sit unapplied until
//     something else poked the CR.
//   - The peer MysqlDatabase watch is what makes ownership arbitration
//     converge: a higher-ranked peer appearing, leaving, or changing its
//     claim must re-run the arbitration of every CR it outranks, including
//     ones already Ready.
func (r *MysqlDatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Index CRs by every Secret they reference — the owner's and each
	// users[] entry's — so the Secret watch maps with an O(1) cache lookup.
	// Secrets are the churniest resource in most clusters (Helm releases,
	// cert renewals, token rotation); the map func runs for every one of
	// those events and must not List-and-scan each time. The multi-value
	// index is also what makes users[] rotation a Secret write and nothing
	// else.
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1alpha1.MysqlDatabase{},
		mdbSecretNamesIndex, mdbSecretNames); err != nil {
		return fmt.Errorf("index mysqldatabase secret names: %w", err)
	}
	// Index CRs by groupRef.name so the group watch and the peer watch map
	// to exactly the matching tenants instead of scanning the namespace.
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1alpha1.MysqlDatabase{},
		mdbGroupRefIndex, func(o client.Object) []string {
			return []string{o.(*v1alpha1.MysqlDatabase).Spec.GroupRef.Name}
		}); err != nil {
		return fmt.Errorf("index mysqldatabase group ref: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{MaxConcurrentReconciles: mysqlDatabaseMaxConcurrentReconciles}).
		For(&v1alpha1.MysqlDatabase{}).
		Watches(&v1alpha1.MysqlFailoverGroup{},
			handler.EnqueueRequestsFromMapFunc(r.mapGroupToDatabases),
			builder.WithPredicates(groupActiveSiteChangedPredicate())).
		Watches(&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.mapSecretToDatabases),
			builder.WithPredicates(secretDataChangedPredicate())).
		Watches(&v1alpha1.MysqlDatabase{},
			handler.EnqueueRequestsFromMapFunc(r.mapDatabaseToPeers),
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}

// mdbSecretNamesIndex is the cache index key for every Secret a
// MysqlDatabase references: spec.owner.secretName plus each
// spec.users[].secretName.
const mdbSecretNamesIndex = ".mysqldatabase.secretNames"

// mdbGroupRefIndex is the cache index key for spec.groupRef.name.
const mdbGroupRefIndex = ".spec.groupRef.name"

// secretDataChangedPredicate drops Secret update events whose Data did not
// change. ESO and friends re-apply Secrets on their refresh interval whether
// or not the value rotated; without this, every no-op re-apply of an owner
// Secret would enqueue a full reconcile for its tenant. Creates and deletes
// pass through (the zero predicate.Funcs returns true), because a Secret
// appearing is exactly what un-parks an OwnerSecretMissing tenant.
func secretDataChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldSecret, okOld := e.ObjectOld.(*corev1.Secret)
			newSecret, okNew := e.ObjectNew.(*corev1.Secret)
			if !okOld || !okNew {
				return false
			}
			return !reflect.DeepEqual(oldSecret.Data, newSecret.Data)
		},
	}
}

// groupActiveSiteChangedPredicate narrows the group watch to the transitions
// a MysqlDatabase actually cares about: the primary moved, or the group
// entered/left a fenced state. Without it, every heartbeat status write on a
// busy MysqlFailoverGroup would fan out to every tenant CR in the namespace.
func groupActiveSiteChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldFG, okOld := e.ObjectOld.(*v1alpha1.MysqlFailoverGroup)
			newFG, okNew := e.ObjectNew.(*v1alpha1.MysqlFailoverGroup)
			if !okOld || !okNew {
				return false
			}
			if oldFG.Status.ActiveSite != newFG.Status.ActiveSite {
				return true
			}
			oldReason, _, oldFenced := groupFenced(oldFG)
			newReason, _, newFenced := groupFenced(newFG)
			return oldFenced != newFenced || oldReason != newReason
		},
	}
}

// mapGroupToDatabases resolves the group watch through the groupRef index:
// a failover fans out to exactly the tenants on that group.
func (r *MysqlDatabaseReconciler) mapGroupToDatabases(ctx context.Context, obj client.Object) []reconcile.Request {
	var list v1alpha1.MysqlDatabaseList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace()),
		client.MatchingFields{mdbGroupRefIndex: obj.GetName()}); err != nil {
		log.FromContext(ctx).Error(err, "list mysqldatabases for group watch mapping", "namespace", obj.GetNamespace())
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		reqs = append(reqs, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: list.Items[i].Namespace, Name: list.Items[i].Name},
		})
	}
	return reqs
}

func (r *MysqlDatabaseReconciler) mapSecretToDatabases(ctx context.Context, obj client.Object) []reconcile.Request {
	var list v1alpha1.MysqlDatabaseList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace()),
		client.MatchingFields{mdbSecretNamesIndex: obj.GetName()}); err != nil {
		log.FromContext(ctx).Error(err, "list mysqldatabases for secret watch mapping", "namespace", obj.GetNamespace())
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		reqs = append(reqs, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: list.Items[i].Namespace, Name: list.Items[i].Name},
		})
	}
	return reqs
}

// mapDatabaseToPeers enqueues every other MysqlDatabase on the same group
// when a CR's spec changes (or it is created or deleted). This is the
// convergence mechanism for ownership arbitration: the CR that gains or
// loses a claim does not need to be the one that re-runs — its peers do.
func (r *MysqlDatabaseReconciler) mapDatabaseToPeers(ctx context.Context, obj client.Object) []reconcile.Request {
	mdb, ok := obj.(*v1alpha1.MysqlDatabase)
	if !ok {
		return nil
	}
	var list v1alpha1.MysqlDatabaseList
	if err := r.List(ctx, &list, client.InNamespace(mdb.Namespace),
		client.MatchingFields{mdbGroupRefIndex: mdb.Spec.GroupRef.Name}); err != nil {
		log.FromContext(ctx).Error(err, "list mysqldatabases for peer watch mapping", "namespace", mdb.Namespace)
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		item := &list.Items[i]
		if item.Name == mdb.Name {
			continue
		}
		reqs = append(reqs, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: item.Namespace, Name: item.Name},
		})
	}
	return reqs
}
