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

// errPreExistingOwnerUser reports an owner username that already exists in
// MySQL without being recorded as this CR's owner. CREATE USER IF NOT EXISTS
// + ALTER USER would otherwise reset the password of an account some other
// system created — and a later deletionPolicy: Delete would drop it.
type errPreExistingOwnerUser struct {
	username string
}

func (e *errPreExistingOwnerUser) Error() string {
	return fmt.Sprintf("MySQL account %q already exists and is not recorded as this CR's owner (status.ownerUser); refusing to manage an account Bloodraven did not create", e.username)
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
	if ownerUser == "" || ownerPass == "" {
		return r.pending(ctx, &mdb, "OwnerSecretIncomplete",
			fmt.Sprintf("Secret %q must carry non-empty username and password keys", secretKey.Name))
	}

	// Validate everything that ends up in SQL before anything is rendered.
	// The owner username in particular cannot be validated by the API
	// server, because it arrives from a Secret.
	if err := mdb.Spec.Validate(ownerUser); err != nil {
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

	currentHash, err := computeDatabaseHash(&mdb, &ownerSecret, &fg)
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

	// Capture the write-ahead record as it stood before this reconcile:
	// applyDatabase needs the pre-stamp values to decide what this CR may
	// adopt and what it must revoke.
	prior := priorApplyState{
		databaseCreated:  mdb.Status.DatabaseCreated,
		recordedOwner:    mdb.Status.OwnerUser,
		pendingOwnerUser: mdb.Status.PendingOwnerUser,
		appliedGrants:    mdb.Status.AppliedGrants,
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

	// The Creating stamp doubles as the write-ahead record for deletion:
	// DatabaseCreated and OwnerUser are committed *after* the connection is
	// open and *before* any SQL executes, so a partially-applied CR (say,
	// one that failed on GrantUserMissing after the owner user was created)
	// still knows what it may have touched when deletionPolicy: Delete
	// needs to clean up — while a CR that never even connected cannot
	// authorize dropping same-named objects it provably never touched.
	// OwnerUser is only written here when it is empty: during a username
	// rotation the previous name must survive in status until the old
	// account is actually dropped, or a failed rotation would leak a live
	// privileged user with no record of it anywhere.
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseCreating || mdb.Status.OwnerUser == "" {
		if err := r.stampStatus(ctx, &mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
			st.Phase = v1alpha1.MysqlDatabasePhaseCreating
			st.ObservedGeneration = mdb.Generation
			st.DatabaseCreated = true
			if st.OwnerUser == "" {
				st.OwnerUser = ownerUser
			} else if st.OwnerUser != ownerUser {
				// Username rotation: record the new name BEFORE any
				// rotation SQL. A failure between creating the new
				// account and the Ready stamp must not leave the next
				// reconcile unable to attribute it (see
				// Status.PendingOwnerUser).
				st.PendingOwnerUser = ownerUser
			}
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
	}

	// Rotation write-ahead for the retries the Creating stamp skips (phase
	// already Creating with a recorded owner): the pending record must
	// always name the user this run is about to create or re-apply, or the
	// adoption gate cannot attribute it after a stamp failure. Also covers
	// the Secret changing again mid-rotation.
	if mdb.Status.OwnerUser != "" && mdb.Status.OwnerUser != ownerUser &&
		mdb.Status.PendingOwnerUser != ownerUser {
		if err := r.stampStatus(ctx, &mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
			st.PendingOwnerUser = ownerUser
		}); err != nil {
			return ctrl.Result{}, err
		}
		mdb.Status.PendingOwnerUser = ownerUser
		prior.pendingOwnerUser = ownerUser
	}

	appliedGrants, err := applyDatabase(sqlCtx, db, &mdb, ownerUser, ownerPass, prior)
	if err != nil {
		var preExists *errDatabasePreExists
		if errors.As(err, &preExists) {
			// The refusal ran no SQL at all, so the write-ahead stamp the
			// Creating pass just committed does not describe anything this
			// CR touched. Restore it, or a later deletionPolicy: Delete
			// would DROP the very schema/user this CR refused to adopt.
			if serr := r.stampStatus(ctx, &mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
				st.DatabaseCreated = prior.databaseCreated
				st.OwnerUser = prior.recordedOwner
			}); serr != nil {
				return ctrl.Result{}, serr
			}
			return r.fail(ctx, &mdb, "DatabasePreExists", err.Error())
		}
		var preUser *errPreExistingOwnerUser
		if errors.As(err, &preUser) {
			// Same contract for the owner: when this CR never recorded an
			// owner (first apply), the refused account is provably not
			// ours, so the record must not name it. A rotation into an
			// existing username keeps the old recorded owner — that one IS
			// ours and must stay covered by cleanup.
			if prior.recordedOwner == "" {
				if serr := r.stampStatus(ctx, &mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
					st.OwnerUser = ""
				}); serr != nil {
					return ctrl.Result{}, serr
				}
			}
			return r.fail(ctx, &mdb, "PreExistingOwnerUser", err.Error())
		}
		var missing *errGrantUserMissing
		if errors.As(err, &missing) {
			return r.fail(ctx, &mdb, "GrantUserMissing", err.Error())
		}
		if transientSQLError(err) {
			logger.V(1).Info("transient MySQL error, staying pending", "error", err)
			return r.pending(ctx, &mdb, "PrimaryUnavailable",
				fmt.Sprintf("transient MySQL error on group %q: %v", fg.Name, err))
		}
		return r.fail(ctx, &mdb, "MySQLError", err.Error())
	}

	// A rotated owner username means the previously-recorded account is
	// obsolete desired state. The drop happens now — after the new owner is
	// created, granted, and applied — so rotation is create-before-drop: a
	// failure mid-handover leaves both accounts alive and retried, never a
	// window in which the tenant has no owner at all. status kept the old
	// name through the apply (see the Creating stamp), so a transient
	// failure here retries rather than leaking the account.
	if prev := prior.recordedOwner; prev != "" && prev != ownerUser {
		switch {
		case reserved[prev]:
			// The old name became a group-level principal since the CR
			// last applied; dropping it would drop the group's own
			// credential. Leave it and say so.
			r.Recorder.Eventf(&mdb, corev1.EventTypeWarning, "OwnerUserReservedSkipped",
				"previous owner user %q is now a group-level principal of %q; not dropping it during rotation to %q",
				prev, fg.Name, ownerUser)
		default:
			if referrer, claimed, cerr := r.siblingGrantsClaim(ctx, &mdb, prev); cerr != nil {
				return ctrl.Result{}, cerr
			} else if claimed {
				r.Recorder.Eventf(&mdb, corev1.EventTypeWarning, "OwnerUserDropSkipped",
					"previous owner user %q is listed in MysqlDatabase %q's spec.grants[]; not dropping it during rotation to %q",
					prev, referrer, ownerUser)
				break
			}
			dropStmt, derr := renderDropUser(prev)
			if derr != nil {
				// A status value that no longer renders is unreachable via
				// any input we accept; log and move on rather than wedging
				// the CR.
				logger.Error(derr, "cannot render drop for previous owner user; skipping", "previousOwner", prev)
			} else {
				if _, xerr := db.ExecContext(sqlCtx, dropStmt); xerr != nil {
					if transientSQLError(xerr) {
						return r.pending(ctx, &mdb, "PrimaryUnavailable",
							fmt.Sprintf("transient MySQL error dropping rotated owner user on group %q: %v", fg.Name, xerr))
					}
					return r.fail(ctx, &mdb, "MySQLError",
						fmt.Sprintf("drop previous owner user %q: %v", prev, xerr))
				}
				r.Recorder.Eventf(&mdb, corev1.EventTypeNormal, "OwnerUserRotated",
					"created and granted new owner user %q, then dropped previous owner user %q", ownerUser, prev)
			}
		}
	}

	message := fmt.Sprintf("database %s ready on site %s", mdb.Spec.DatabaseName, fg.Status.ActiveSite)
	if err := r.stampStatus(ctx, &mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
		st.Phase = v1alpha1.MysqlDatabasePhaseReady
		st.ObservedGeneration = mdb.Generation
		st.DatabaseCreated = true
		st.OwnerUser = ownerUser
		st.PendingOwnerUser = ""
		st.AppliedGrants = appliedGrants
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
			mdb.Spec.DatabaseName, strings.Join(dropOwners, ", "), fg.Status.ActiveSite)
	case dropDB:
		r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "DatabaseDropped",
			"deletionPolicy=Delete: dropped database %q on site %s (owner user left untouched)",
			mdb.Spec.DatabaseName, fg.Status.ActiveSite)
	default:
		r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "DatabaseDropped",
			"deletionPolicy=Delete: dropped user(s) %q on site %s (database left untouched)",
			strings.Join(dropOwners, ", "), fg.Status.ActiveSite)
	}
	logger.Info("dropped tenant database", "database", mdb.Spec.DatabaseName, "group", fg.Name)

	return ctrl.Result{}, r.removeFinalizer(ctx, mdb)
}

// deleteScope decides which MySQL objects the deleting CR may remove: the
// database (unless another live CR on the same group still declares it) and
// the owner user(s) — unless the username is reserved, another live CR
// shares the Secret or the username, or a sibling CR still lists the user
// in spec.grants[]. It returns the owner usernames that may be dropped;
// that is plural on purpose: a rotation that crashed after creating the new
// account but before status caught up leaves the new username recorded only
// in the Secret, and Delete must clean up both or leak a privileged user.
// CRs that are themselves being deleted don't count as claims — of two CRs
// deleted together, the first to reconcile drops, and the second's
// statements are IF EXISTS no-ops.
func (r *MysqlDatabaseReconciler) deleteScope(ctx context.Context, mdb *v1alpha1.MysqlDatabase, reserved map[string]bool) (dropDB bool, dropOwners []string, err error) {
	dropDB = true

	// Candidate owner usernames: the recorded one, plus — only when this CR
	// actually owns a recorded owner — whatever the Secret currently names
	// (the crashed-rotation case; see the function comment). Gating the
	// Secret candidate on a non-empty record is load-bearing: a CR that
	// refused to adopt a pre-existing account must not drop it via the
	// Secret back door.
	candidates := []string{}
	if mdb.Status.OwnerUser != "" {
		candidates = append(candidates, mdb.Status.OwnerUser)
		if secretUser, rerr := r.ownerUsernameFromSecret(ctx, mdb); rerr == nil &&
			secretUser != "" && secretUser != mdb.Status.OwnerUser {
			candidates = append(candidates, secretUser)
		}
	}
	// The rotation write-ahead record names an account that may exist in
	// MySQL even though status.ownerUser never advanced to it.
	if mdb.Status.PendingOwnerUser != "" && mdb.Status.PendingOwnerUser != mdb.Status.OwnerUser {
		candidates = append(candidates, mdb.Status.PendingOwnerUser)
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

	for _, candidate := range candidates {
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
		dropOwners = append(dropOwners, candidate)
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

// siblingGrantsClaim scans live sibling CRs on the same group for a
// spec.grants[] entry naming username. Dropping a principal some other CR
// still grants would leave that CR falsely Ready with a dead user, so the
// drop is skipped and the referrer named.
func (r *MysqlDatabaseReconciler) siblingGrantsClaim(ctx context.Context, mdb *v1alpha1.MysqlDatabase, username string) (referrer string, claimed bool, err error) {
	var list v1alpha1.MysqlDatabaseList
	if err := r.List(ctx, &list, client.InNamespace(mdb.Namespace)); err != nil {
		return "", false, fmt.Errorf("list mysqldatabases for grants guard: %w", err)
	}
	referrer, claimed = siblingGrantsClaimIn(&list, mdb.Name, mdb.Spec.GroupRef.Name, username)
	return referrer, claimed, nil
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

// priorApplyState is the write-ahead record as it stood before the current
// reconcile stamped it. applyDatabase needs the pre-stamp values: the
// adoption gates must judge "did this CR create that schema/user" by what
// was recorded before this run committed to executing DDL, and revocation
// of removed grants[] entries needs the previous apply's username list.
type priorApplyState struct {
	databaseCreated  bool
	recordedOwner    string
	pendingOwnerUser string
	appliedGrants    []string
}

// applyDatabase runs the idempotent apply sequence on an open admin
// connection and returns the usernames granted, owner first.
//
// Every statement is rendered — and therefore validated — before any of them
// executes, so a bad identifier cannot produce a partially-applied tenant.
// The deliberately interleaved checks are the existence queries: the schema
// adoption gate before the CREATE, the owner adoption gate before the
// CREATE USER, and each spec.grants[] check immediately before that user's
// GRANT, which must abort rather than fall through to a CREATE USER.
//
// Privilege application is grant-then-revoke: the desired set is GRANTed
// first and only the surplus revoked afterwards, so a failure mid-sequence
// leaves the principal over-granted for one requeue interval rather than
// with zero privileges on its own live database.
func applyDatabase(ctx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase, ownerUser, ownerPass string, prior priorApplyState) ([]string, error) {
	spec := &mdb.Spec

	createDB, err := renderCreateDatabase(spec.DatabaseName, spec.EffectiveCharacterSet(), spec.EffectiveCollation())
	if err != nil {
		return nil, err
	}
	alterDB, err := renderAlterDatabase(spec.DatabaseName, spec.EffectiveCharacterSet(), spec.EffectiveCollation())
	if err != nil {
		return nil, err
	}
	ownerStmts, err := renderOwnerUserStatements(ownerUser, ownerPass)
	if err != nil {
		return nil, err
	}
	ownerPrivs := spec.EffectiveOwnerPrivileges()
	ownerGrant, err := renderGrant("spec.owner.privileges", ownerPrivs, spec.DatabaseName, ownerUser)
	if err != nil {
		return nil, err
	}
	ownerSurplus, err := renderRevokeSurplus("spec.owner secret username", ownerPrivs, spec.DatabaseName, ownerUser)
	if err != nil {
		return nil, err
	}
	grantStmts := make([]string, len(spec.Grants))
	grantSurplus := make([]string, len(spec.Grants))
	for i, g := range spec.Grants {
		stmt, err := renderGrant(fmt.Sprintf("spec.grants[%d].privileges", i), g.Privileges, spec.DatabaseName, g.Username)
		if err != nil {
			return nil, err
		}
		grantStmts[i] = stmt
		surplus, err := renderRevokeSurplus(fmt.Sprintf("spec.grants[%d].username", i), g.Privileges, spec.DatabaseName, g.Username)
		if err != nil {
			return nil, err
		}
		grantSurplus[i] = surplus
	}
	// Entries removed from spec.grants[] since the last successful apply
	// get revoked: grants[] is desired state on the way out as well as on
	// the way in. The usernames come from status.appliedGrants, which is
	// exactly why that field exists.
	current := make(map[string]bool, len(spec.Grants)+1)
	current[ownerUser] = true
	for _, g := range spec.Grants {
		current[g.Username] = true
	}
	var removedRevokes []string
	for _, user := range prior.appliedGrants {
		if current[user] {
			continue
		}
		stmt, err := renderRevokeAll("status.appliedGrants entry", spec.DatabaseName, user)
		if err != nil {
			return nil, err
		}
		removedRevokes = append(removedRevokes, stmt)
	}

	exec := func(stmt string) error {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %s: %w", credentialStatementErrorLabel(mdb.Name, stmt), err)
		}
		return nil
	}

	// Adoption gate: a schema that already exists is only this CR's to
	// manage when this CR created it. Without the gate, CREATE DATABASE
	// IF NOT EXISTS would silently adopt a foreign schema, grant tenant
	// principals onto it, and later authorize its DROP.
	dbExists, err := schemaExists(ctx, db, spec.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("check schema existence: %w", err)
	}
	if dbExists && !prior.databaseCreated {
		return nil, &errDatabasePreExists{database: spec.DatabaseName}
	}
	if !dbExists {
		if err := exec(createDB); err != nil {
			return nil, err
		}
	}
	// characterSet/collation are mutable desired state; CREATE DATABASE IF
	// NOT EXISTS would never apply an edit to an existing schema, so the
	// ALTER runs on every apply. It only changes schema defaults.
	if err := exec(alterDB); err != nil {
		return nil, err
	}

	// Owner adoption gate: a FIRST apply must never adopt an account some
	// other system created — CREATE USER IF NOT EXISTS + ALTER USER would
	// reset its password and inherit its cross-schema privileges. An
	// account matching the recorded owner is ours; so is one matching the
	// rotation write-ahead record (status.pendingOwnerUser), which is how a
	// rotation whose Ready stamp failed proves to the next reconcile that
	// the already-created new account belongs to this CR instead of
	// wedging on PreExistingOwnerUser.
	ownerExists, err := mysqlUserExists(ctx, db, ownerUser)
	if err != nil {
		return nil, fmt.Errorf("check owner user existence: %w", err)
	}
	if ownerExists && prior.recordedOwner != ownerUser && prior.pendingOwnerUser != ownerUser {
		return nil, &errPreExistingOwnerUser{username: ownerUser}
	}
	for _, stmt := range ownerStmts {
		if err := exec(stmt); err != nil {
			return nil, err
		}
	}
	if err := exec(ownerGrant); err != nil {
		return nil, err
	}
	if ownerSurplus != "" {
		if err := exec(ownerSurplus); err != nil {
			return nil, err
		}
	}

	applied := []string{ownerUser}
	for i, g := range spec.Grants {
		exists, err := mysqlUserExists(ctx, db, g.Username)
		if err != nil {
			return nil, fmt.Errorf("check spec.grants[%d] user: %w", i, err)
		}
		if !exists {
			return nil, &errGrantUserMissing{username: g.Username}
		}
		if err := exec(grantStmts[i]); err != nil {
			return nil, err
		}
		if grantSurplus[i] != "" {
			if err := exec(grantSurplus[i]); err != nil {
				return nil, err
			}
		}
		applied = append(applied, g.Username)
	}

	for _, stmt := range removedRevokes {
		if err := exec(stmt); err != nil {
			return nil, err
		}
	}

	return applied, nil
}

// dropDatabase is the deletionPolicy: Delete path. It revokes before it
// drops, because MySQL leaves schema-level grant rows behind when a schema
// disappears, and it never drops a spec.grants[] user: those principals are
// shared and this CRD did not create them. dropDB and dropOwners come from
// deleteScope — the schema drop is suppressed when another live CR still
// declares the database, and each owner username is individually vetted.
func dropDatabase(ctx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase, dropDB bool, dropOwners []string) error {
	spec := &mdb.Spec

	stmts := make([]string, 0, len(spec.Grants)+len(mdb.Status.AppliedGrants)+len(dropOwners)+2)
	if dropDB {
		// Revoke the union of the grants currently declared and the ones
		// recorded by earlier applies: an entry removed from spec.grants[]
		// must not survive the delete as a lingering mysql.db row that
		// reactivates if the schema name is ever recreated.
		revoked := make(map[string]bool)
		revokeFor := func(kind, username string) error {
			if revoked[username] {
				return nil
			}
			revoked[username] = true
			stmt, err := renderRevokeAll(kind, spec.DatabaseName, username)
			if err != nil {
				return err
			}
			stmts = append(stmts, stmt)
			return nil
		}
		for i, g := range spec.Grants {
			if err := revokeFor(fmt.Sprintf("spec.grants[%d].username", i), g.Username); err != nil {
				return err
			}
		}
		for _, username := range mdb.Status.AppliedGrants {
			if err := revokeFor("status.appliedGrants entry", username); err != nil {
				return err
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
	// so even a partially-applied owner is covered, and the Secret-derived
	// candidate covers a rotation that crashed mid-handover.
	for _, username := range dropOwners {
		dropUser, err := renderDropUser(username)
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

// mysqlUserExists answers the spec.grants[] precondition with a
// parameterized query — the username is compared, never rendered.
func mysqlUserExists(ctx context.Context, db *sql.DB, username string) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, grantUserExistsQuery, username, tenantUserHost).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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
func computeDatabaseHash(mdb *v1alpha1.MysqlDatabase, ownerSecret *corev1.Secret, fg *v1alpha1.MysqlFailoverGroup) (string, error) {
	h := sha256.New()

	specJSON, err := json.Marshal(mdb.Spec)
	if err != nil {
		return "", fmt.Errorf("marshal spec: %w", err)
	}
	fmt.Fprintf(h, "spec=%s\n", specJSON)
	fmt.Fprintf(h, "activeSite=%s\n", fg.Status.ActiveSite)
	fmt.Fprintf(h, "secret=%s/%s\n", ownerSecret.UID, ownerSecret.ResourceVersion)
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
	// Index CRs by owner Secret name so the Secret watch maps with an O(1)
	// cache lookup. Secrets are the churniest resource in most clusters
	// (Helm releases, cert renewals, token rotation); the map func runs for
	// every one of those events and must not List-and-scan each time.
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1alpha1.MysqlDatabase{},
		mdbOwnerSecretIndex, func(o client.Object) []string {
			return []string{o.(*v1alpha1.MysqlDatabase).Spec.Owner.SecretName}
		}); err != nil {
		return fmt.Errorf("index mysqldatabase owner secret: %w", err)
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

// mdbOwnerSecretIndex is the cache index key for spec.owner.secretName.
const mdbOwnerSecretIndex = ".spec.owner.secretName"

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
		client.MatchingFields{mdbOwnerSecretIndex: obj.GetName()}); err != nil {
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
