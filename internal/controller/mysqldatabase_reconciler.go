package controller

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

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

	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseCreating {
		if err := r.stampStatus(ctx, &mdb, func(st *v1alpha1.MysqlDatabaseStatus) {
			st.Phase = v1alpha1.MysqlDatabasePhaseCreating
			st.ObservedGeneration = mdb.Generation
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

	appliedGrants, err := applyDatabase(ctx, db, &mdb, ownerUser, ownerPass)
	if err != nil {
		var missing *errGrantUserMissing
		if errors.As(err, &missing) {
			r.Recorder.Eventf(&mdb, corev1.EventTypeWarning, "GrantUserMissing",
				"MySQL user %q does not exist; no user was created", missing.username)
			return r.fail(ctx, &mdb, "GrantUserMissing", err.Error())
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

	logger := log.FromContext(ctx)

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

	db, err := openAdminConnection(ctx, r.Client, &fg, r.dialer())
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("connect to primary for deletion: %w", err)
	}
	defer db.Close()

	if err := dropDatabase(ctx, db, mdb); err != nil {
		return ctrl.Result{}, fmt.Errorf("drop tenant database: %w", err)
	}

	r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "DatabaseDropped",
		"deletionPolicy=Delete: dropped database %q and user %q on site %s",
		mdb.Spec.DatabaseName, mdb.Status.OwnerUser, fg.Status.ActiveSite)
	logger.Info("dropped tenant database", "database", mdb.Spec.DatabaseName, "group", fg.Name)

	return ctrl.Result{}, r.removeFinalizer(ctx, mdb)
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
// shared and this CRD did not create them.
func dropDatabase(ctx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase) error {
	spec := &mdb.Spec

	stmts := make([]string, 0, len(spec.Grants)+3)
	for i, g := range spec.Grants {
		stmt, err := renderRevokeAll(fmt.Sprintf("spec.grants[%d].username", i), spec.DatabaseName, g.Username)
		if err != nil {
			return err
		}
		stmts = append(stmts, stmt)
	}
	dropDB, err := renderDropDatabase(spec.DatabaseName)
	if err != nil {
		return err
	}
	stmts = append(stmts, dropDB)

	// The owner user is only dropped when we know which user it was. An
	// empty status.ownerUser means the CR never reached a successful apply,
	// so there is no owner of ours to remove.
	if mdb.Status.OwnerUser != "" {
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
	rows, err := db.QueryContext(ctx, grantUserExistsQuery, username, tenantUserHost)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	found := rows.Next()
	if err := rows.Err(); err != nil {
		return false, err
	}
	return found, nil
}

// groupFenced reports whether the group's primary is currently fenced by an
// in-place restore or a planned failover, in which case a MysqlDatabase backs
// off to Pending instead of erroring.
func groupFenced(fg *v1alpha1.MysqlFailoverGroup) (reason, message string, fenced bool) {
	if rip := fg.Status.RestoreInPlace; rip != nil && restoreInPlaceActive(rip.Phase) {
		return "RestoreInProgress",
			fmt.Sprintf("group %q is mid in-place restore (phase %s)", fg.Name, rip.Phase), true
	}
	if pf := fg.Status.PlannedFailover; pf != nil && plannedFailoverActive(pf.Phase) {
		return "PlannedFailoverInProgress",
			fmt.Sprintf("group %q is mid planned failover (phase %s)", fg.Name, pf.Phase), true
	}
	return "", "", false
}

func restoreInPlaceActive(phase v1alpha1.RestoreInPlacePhase) bool {
	switch phase {
	case v1alpha1.RestoreInPlaceNone,
		v1alpha1.RestoreInPlaceSucceeded,
		v1alpha1.RestoreInPlaceFailed:
		return false
	default:
		return true
	}
}

func plannedFailoverActive(phase v1alpha1.PlannedFailoverPhase) bool {
	switch phase {
	case v1alpha1.PlannedFailoverPhaseNone,
		v1alpha1.PlannedFailoverPhaseDeferred,
		v1alpha1.PlannedFailoverPhaseSucceeded,
		v1alpha1.PlannedFailoverPhaseFailed:
		return false
	default:
		return true
	}
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
func (r *MysqlDatabaseReconciler) fail(ctx context.Context, mdb *v1alpha1.MysqlDatabase, reason, message string) (ctrl.Result, error) {
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.MysqlDatabase{}).
		Watches(&v1alpha1.MysqlFailoverGroup{},
			handler.EnqueueRequestsFromMapFunc(r.mapGroupToDatabases),
			builder.WithPredicates(groupActiveSiteChangedPredicate())).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapSecretToDatabases)).
		Complete(r)
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
	return r.databasesMatching(ctx, obj.GetNamespace(), func(mdb *v1alpha1.MysqlDatabase) bool {
		return mdb.Spec.Owner.SecretName == obj.GetName()
	})
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
