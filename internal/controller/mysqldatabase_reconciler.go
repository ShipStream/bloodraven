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
	"net"
	"reflect"
	"sort"
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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// mysqlDatabaseFinalizer guards the MySQL-side cleanup decision. Named to
// match the existing finalizers in this repo (shipstream.io/mysqlbackup,
// shipstream.io/mysqlbackup-verification).
const mysqlDatabaseFinalizer = "shipstream.io/mysqldatabase"

// ConditionDatabaseReady is the Ready condition type on
// MysqlDatabase.status.conditions. Together with status.observedGeneration
// it is the contract a caller polls instead of opening a MySQL connection,
// so treat both as API surface rather than as diagnostics.
const ConditionDatabaseReady = "Ready"

// Requeue intervals. Pending states are ordering problems that resolve on
// their own; Failed states need a human but are re-checked in case the human
// fixed the world rather than the CR (e.g. created the missing grant user).
const (
	mysqlDatabasePendingRequeue = 30 * time.Second
	mysqlDatabaseFailedRequeue  = 60 * time.Second
)

// errGrantUserMissing reports a spec.grants[] entry naming a MySQL user that
// does not exist. It is a distinct type because the response is specific:
// fail the CR loudly with reason GrantUserMissing and, above all, do not
// create the user. A MysqlDatabase that could conjure arbitrary MySQL
// principals would be a privilege-escalation primitive.
type errGrantUserMissing struct {
	username string
}

func (e *errGrantUserMissing) Error() string {
	return fmt.Sprintf("MySQL user %q does not exist; spec.grants[] never creates users", e.username)
}

// transientSQLError distinguishes connectivity weather from a MySQL verdict.
// It exists because an unplanned failover can land between the dial and the
// exec: the group watch re-enqueues every tenant the moment ActiveSite moves,
// which can be up to 30s before the promoted site is actually writable
// (pendingPromotionActiveSiteTTL). DDL against that primary fails with 1290;
// a connection killed mid-promotion surfaces as a driver/net error. Neither
// is a fact about the CR, so neither may latch Phase=Failed — a healthy
// tenant must not go red for every ordinary failover.
func transientSQLError(err error) bool {
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

	if !controllerutil.ContainsFinalizer(&mdb, mysqlDatabaseFinalizer) {
		controllerutil.AddFinalizer(&mdb, mysqlDatabaseFinalizer)
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

	currentHash, err := computeDatabaseHash(&mdb, &ownerSecret, fg.Status.ActiveSite)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("compute database hash: %w", err)
	}

	// Skip if nothing that matters changed. The active site is part of the
	// hash, so a failover invalidates it and forces a re-apply against the
	// new primary — which is why the group watch below is correct rather
	// than merely helpful.
	if mdb.Status.Phase == v1alpha1.MysqlDatabasePhaseReady &&
		mdb.Status.LastAppliedHash == currentHash &&
		mdb.Status.ObservedGeneration == mdb.Generation {
		return ctrl.Result{}, nil
	}

	// Refuse to adopt a group-level principal as a tenant owner. Checked
	// after the hash short-circuit so a settled cluster does not pay for the
	// extra Secret reads, and before anything is rendered so the offending
	// ALTER USER is never built. The check fails closed: a group Secret that
	// is mid-rotation (NotFound) parks the tenant in Pending, and any other
	// read error fails the reconcile — never proceed on a partial reserved
	// set. See reservedGroupUsernames.
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
	conflictReason, conflictMsg, err := r.ownershipConflict(ctx, &mdb, ownerUser)
	if err != nil {
		return ctrl.Result{}, err
	}
	if conflictReason != "" {
		return r.fail(ctx, &mdb, conflictReason, conflictMsg)
	}

	// The Creating stamp doubles as the write-ahead record for deletion:
	// DatabaseCreated and OwnerUser are committed *before* any SQL executes,
	// so a partially-applied CR (say, one that failed on GrantUserMissing
	// after the owner user was created) still knows what it may have touched
	// when deletionPolicy: Delete needs to clean up. OwnerUser is only
	// written here when it is empty: during a username rotation the previous
	// name must survive in status until the old account is actually dropped,
	// or a failed rotation would leak a live privileged user with no record
	// of it anywhere.
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseCreating || mdb.Status.OwnerUser == "" {
		if err := r.stampStatus(ctx, &mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
			st.Phase = v1alpha1.MysqlDatabasePhaseCreating
			st.ObservedGeneration = mdb.Generation
			st.DatabaseCreated = true
			if st.OwnerUser == "" {
				st.OwnerUser = ownerUser
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

	db, err := openAdminConnection(ctx, r.Client, &fg, r.dialer())
	if err != nil {
		// Connectivity is transient by nature; stay Pending and let the
		// controller back off rather than declaring the tenant broken.
		logger.V(1).Info("admin connection unavailable", "error", err)
		return r.pending(ctx, &mdb, "PrimaryUnavailable",
			fmt.Sprintf("cannot reach the primary of group %q: %v", fg.Name, err))
	}
	defer db.Close()

	// A rotated owner username means the previously-recorded account is now
	// obsolete desired state: drop it before applying the new one, so a
	// credential rotation actually revokes the old credential. status keeps
	// the old name until this succeeds (see the Creating stamp above), so a
	// transient failure here retries rather than leaking the account.
	if prev := mdb.Status.OwnerUser; prev != "" && prev != ownerUser && !reserved[prev] {
		dropStmt, derr := renderDropUser(prev)
		if derr != nil {
			// A status value that no longer renders is unreachable via any
			// input we accept; log and move on rather than wedging the CR.
			logger.Error(derr, "cannot render drop for previous owner user; skipping", "previousOwner", prev)
		} else {
			if _, xerr := db.ExecContext(ctx, dropStmt); xerr != nil {
				if transientSQLError(xerr) {
					return r.pending(ctx, &mdb, "PrimaryUnavailable",
						fmt.Sprintf("transient MySQL error dropping rotated owner user on group %q: %v", fg.Name, xerr))
				}
				return r.fail(ctx, &mdb, "MySQLError",
					fmt.Sprintf("drop previous owner user %q: %v", prev, xerr))
			}
			r.Recorder.Eventf(&mdb, corev1.EventTypeNormal, "OwnerUserRotated",
				"dropped previous owner user %q after username rotation to %q", prev, ownerUser)
		}
	}

	appliedGrants, err := applyDatabase(ctx, db, &mdb, ownerUser, ownerPass)
	if err != nil {
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

	message := fmt.Sprintf("database %s ready on site %s", mdb.Spec.DatabaseName, fg.Status.ActiveSite)
	if err := r.stampStatus(ctx, &mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
		st.Phase = v1alpha1.MysqlDatabasePhaseReady
		st.ObservedGeneration = mdb.Generation
		st.DatabaseCreated = true
		st.OwnerUser = ownerUser
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
	if !controllerutil.ContainsFinalizer(mdb, mysqlDatabaseFinalizer) {
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
	// stamped before the first statement ever executes. A CR without it
	// (invalid spec, reserved owner, ownership conflict — all fail before
	// SQL) has nothing of its own in MySQL, and must not drop a database
	// that some other CR or system created under the same name.
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
		return ctrl.Result{RequeueAfter: mysqlDatabasePendingRequeue}, nil
	}

	// The apply path backs off while the primary is fenced; injecting a
	// DROP DATABASE into an in-place restore or a planned failover's drain
	// window would be strictly worse than injecting a CREATE.
	if _, msg, fenced := groupFenced(&fg); fenced {
		r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseDropDeferred",
			"%s; deferring DROP of database %q", msg, mdb.Spec.DatabaseName)
		return ctrl.Result{RequeueAfter: mysqlDatabasePendingRequeue}, nil
	}

	// Scope the drop to what is exclusively ours. Another live CR declaring
	// the same database (a conflict that predates its own failure, or a
	// mid-migration duplicate) means the schema is not ours to drop; another
	// live CR claiming the same owner principal means the user is not.
	dropDB, dropOwner, err := r.deleteScope(ctx, mdb)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !dropDB {
		r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "DatabaseDropSkipped",
			"another MysqlDatabase still declares database %q; not dropping it", mdb.Spec.DatabaseName)
	}
	if !dropDB && !dropOwner {
		return ctrl.Result{}, r.removeFinalizer(ctx, mdb)
	}

	db, err := openAdminConnection(ctx, r.Client, &fg, r.dialer())
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("connect to primary for deletion: %w", err)
	}
	defer db.Close()

	if err := dropDatabase(ctx, db, mdb, dropDB, dropOwner); err != nil {
		return ctrl.Result{}, fmt.Errorf("drop tenant database: %w", err)
	}

	r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "DatabaseDropped",
		"deletionPolicy=Delete: dropped database %q and user %q on site %s",
		mdb.Spec.DatabaseName, mdb.Status.OwnerUser, fg.Status.ActiveSite)
	logger.Info("dropped tenant database", "database", mdb.Spec.DatabaseName, "group", fg.Name)

	return ctrl.Result{}, r.removeFinalizer(ctx, mdb)
}

// deleteScope decides which MySQL objects the deleting CR may remove: the
// database (unless another live CR on the same group still declares it) and
// the owner user (unless another live CR shares the Secret or the username).
// CRs that are themselves being deleted don't count as claims — of two CRs
// deleted together, the first to reconcile drops, and the second's statements
// are IF EXISTS no-ops.
func (r *MysqlDatabaseReconciler) deleteScope(ctx context.Context, mdb *v1alpha1.MysqlDatabase) (dropDB, dropOwner bool, err error) {
	dropDB = true
	dropOwner = mdb.Status.OwnerUser != ""

	var list v1alpha1.MysqlDatabaseList
	if err := r.List(ctx, &list, client.InNamespace(mdb.Namespace)); err != nil {
		return false, false, fmt.Errorf("list mysqldatabases for delete guard: %w", err)
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
		if dropOwner && (other.Spec.Owner.SecretName == mdb.Spec.Owner.SecretName ||
			other.Status.OwnerUser == mdb.Status.OwnerUser) {
			dropOwner = false
		}
	}
	return dropDB, dropOwner, nil
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

// ownershipConflict reports whether a higher-ranked live MysqlDatabase on the
// same group already claims this CR's database name or owner principal.
func (r *MysqlDatabaseReconciler) ownershipConflict(ctx context.Context, mdb *v1alpha1.MysqlDatabase, ownerUser string) (reason, message string, err error) {
	var list v1alpha1.MysqlDatabaseList
	if err := r.List(ctx, &list, client.InNamespace(mdb.Namespace)); err != nil {
		return "", "", fmt.Errorf("list mysqldatabases for conflict check: %w", err)
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
			return "DatabaseNameConflict", fmt.Sprintf(
				"MysqlDatabase %q already declares database %q on group %q; two CRs must not manage one database (deletionPolicy: Delete on either would drop the other's data)",
				other.Name, mdb.Spec.DatabaseName, mdb.Spec.GroupRef.Name), nil
		}
		if other.Spec.Owner.SecretName == mdb.Spec.Owner.SecretName ||
			(other.Status.OwnerUser != "" && other.Status.OwnerUser == ownerUser) {
			return "OwnerConflict", fmt.Sprintf(
				"MysqlDatabase %q already owns MySQL user %q on group %q; each MysqlDatabase must own a distinct user (deleting one CR would drop the other's credential)",
				other.Name, ownerUser, mdb.Spec.GroupRef.Name), nil
		}
	}
	return "", "", nil
}

// applyDatabase runs the idempotent apply sequence on an open admin
// connection and returns the usernames granted, owner first.
//
// Every statement is rendered — and therefore validated — before any of them
// executes, so a bad identifier cannot produce a partially-applied tenant.
// The one thing deliberately interleaved is the spec.grants[] existence
// check, which must run immediately before that user's GRANT and must abort
// rather than fall through to a CREATE USER.
func applyDatabase(ctx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase, ownerUser, ownerPass string) ([]string, error) {
	spec := &mdb.Spec

	createDB, err := renderCreateDatabase(spec.DatabaseName, spec.EffectiveCharacterSet(), spec.EffectiveCollation())
	if err != nil {
		return nil, err
	}
	ownerStmts, err := renderOwnerUserStatements(ownerUser, ownerPass)
	if err != nil {
		return nil, err
	}
	// Revoke-then-grant makes spec.owner.privileges true desired state: a
	// narrowed privilege list actually narrows, instead of GRANT silently
	// leaving the wider old set in place while status reports the new one.
	// The owner is this CR's own user, so the revoke cannot take anything
	// away from a principal some other system manages.
	ownerRevoke, err := renderRevokeAll("spec.owner secret username", spec.DatabaseName, ownerUser)
	if err != nil {
		return nil, err
	}
	ownerGrant, err := renderGrant("spec.owner.privileges", spec.EffectiveOwnerPrivileges(), spec.DatabaseName, ownerUser)
	if err != nil {
		return nil, err
	}
	grantStmts := make([]string, len(spec.Grants))
	for i, g := range spec.Grants {
		stmt, err := renderGrant(fmt.Sprintf("spec.grants[%d].privileges", i), g.Privileges, spec.DatabaseName, g.Username)
		if err != nil {
			return nil, err
		}
		grantStmts[i] = stmt
	}

	exec := func(stmt string) error {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %s: %w", credentialStatementErrorLabel(mdb.Name, stmt), err)
		}
		return nil
	}

	if err := exec(createDB); err != nil {
		return nil, err
	}
	for _, stmt := range ownerStmts {
		if err := exec(stmt); err != nil {
			return nil, err
		}
	}
	if err := exec(ownerRevoke); err != nil {
		return nil, err
	}
	if err := exec(ownerGrant); err != nil {
		return nil, err
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
		applied = append(applied, g.Username)
	}

	if err := exec("FLUSH PRIVILEGES"); err != nil {
		return nil, err
	}
	return applied, nil
}

// dropDatabase is the deletionPolicy: Delete path. It revokes before it
// drops, because MySQL leaves schema-level grant rows behind when a schema
// disappears, and it never drops a spec.grants[] user: those principals are
// shared and this CRD did not create them. dropDB and dropOwner come from
// deleteScope — either may be false when another live CR still claims the
// database or the owner principal.
func dropDatabase(ctx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase, dropDB, dropOwner bool) error {
	spec := &mdb.Spec

	stmts := make([]string, 0, len(spec.Grants)+3)
	if dropDB {
		for i, g := range spec.Grants {
			stmt, err := renderRevokeAll(fmt.Sprintf("spec.grants[%d].username", i), spec.DatabaseName, g.Username)
			if err != nil {
				return err
			}
			stmts = append(stmts, stmt)
		}
		dropDBStmt, err := renderDropDatabase(spec.DatabaseName)
		if err != nil {
			return err
		}
		stmts = append(stmts, dropDBStmt)
	}

	// The owner user is only dropped when we know which user it was: the
	// apply path records status.ownerUser before the first statement runs,
	// so even a partially-applied owner is covered.
	if dropOwner && mdb.Status.OwnerUser != "" {
		dropUser, err := renderDropUser(mdb.Status.OwnerUser)
		if err != nil {
			return err
		}
		stmts = append(stmts, dropUser)
	}
	stmts = append(stmts, "FLUSH PRIVILEGES")

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

// computeDatabaseHash fingerprints everything an apply depends on: the spec,
// the owner Secret's contents, and the active site.
//
// The Secret contributes as a hash of its bytes, never the bytes — the same
// construction computeCredentialHash uses, for the same reason.
//
// Including the active site is what makes "re-run after failover" and "skip
// if unchanged" coexist: without it, the skip check would swallow the very
// re-apply the failover watch exists to trigger.
func computeDatabaseHash(mdb *v1alpha1.MysqlDatabase, ownerSecret *corev1.Secret, activeSite string) (string, error) {
	h := sha256.New()

	specJSON, err := json.Marshal(mdb.Spec)
	if err != nil {
		return "", fmt.Errorf("marshal spec: %w", err)
	}
	fmt.Fprintf(h, "spec=%s\n", specJSON)
	fmt.Fprintf(h, "activeSite=%s\n", activeSite)

	keys := make([]string, 0, len(ownerSecret.Data))
	for k := range ownerSecret.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(h, "%s/%s=%x\n", ownerSecret.Name, k, sha256.Sum256(ownerSecret.Data[k]))
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
	return ctrl.Result{RequeueAfter: mysqlDatabasePendingRequeue}, nil
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
	mutate(&mdb.Status)
	if err := r.Status().Patch(ctx, mdb, patch); err != nil {
		return fmt.Errorf("update mysqldatabase status: %w", err)
	}
	return nil
}

func (r *MysqlDatabaseReconciler) removeFinalizer(ctx context.Context, mdb *v1alpha1.MysqlDatabase) error {
	controllerutil.RemoveFinalizer(mdb, mysqlDatabaseFinalizer)
	if err := r.Update(ctx, mdb); err != nil {
		return fmt.Errorf("remove finalizer: %w", err)
	}
	return nil
}

// SetupWithManager registers the reconciler with the manager.
//
// The two Watches are not conveniences:
//
//   - The MysqlFailoverGroup watch is what makes a MysqlDatabase correct
//     across a failover. Grants replicate, but a CR must not report Ready
//     against a stale primary, so every matching CR is re-enqueued when the
//     group's active site (or its fenced state) changes.
//   - The Secret watch is what makes "rotation is a Secret write and nothing
//     else" true. Without it, a rotated password would sit unapplied until
//     something else poked the CR.
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.MysqlDatabase{}).
		Watches(&v1alpha1.MysqlFailoverGroup{},
			handler.EnqueueRequestsFromMapFunc(r.mapGroupToDatabases),
			builder.WithPredicates(groupActiveSiteChangedPredicate())).
		Watches(&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.mapSecretToDatabases),
			builder.WithPredicates(secretDataChangedPredicate())).
		Complete(r)
}

// mdbOwnerSecretIndex is the cache index key for spec.owner.secretName.
const mdbOwnerSecretIndex = ".spec.owner.secretName"

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

func (r *MysqlDatabaseReconciler) mapGroupToDatabases(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.databasesMatching(ctx, obj.GetNamespace(), func(mdb *v1alpha1.MysqlDatabase) bool {
		return mdb.Spec.GroupRef.Name == obj.GetName()
	})
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

func (r *MysqlDatabaseReconciler) databasesMatching(ctx context.Context, namespace string, match func(*v1alpha1.MysqlDatabase) bool) []reconcile.Request {
	var list v1alpha1.MysqlDatabaseList
	if err := r.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		log.FromContext(ctx).Error(err, "list mysqldatabases for watch mapping", "namespace", namespace)
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		item := &list.Items[i]
		if !match(item) {
			continue
		}
		reqs = append(reqs, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: item.Namespace, Name: item.Name},
		})
	}
	return reqs
}
