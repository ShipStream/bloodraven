// Package mysql opens an authenticated connection to a MySQL pod in
// the playground via a port-forwarded SPDY tunnel.
package mysql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

// SiteClient is a port-forwarded MySQL connection to a single site
// pod. Close it when done; long-lived clients across scenario phases
// risk SPDY tunnel drops (see runner design risk #6).
type SiteClient struct {
	Site string
	DB   *sql.DB

	pf *pgkube.PortForward
}

// Close releases the SPDY tunnel and the underlying *sql.DB.
func (c *SiteClient) Close() error {
	var firstErr error
	if c.DB != nil {
		if err := c.DB.Close(); err != nil {
			firstErr = err
		}
	}
	if c.pf != nil {
		c.pf.Stop()
	}
	return firstErr
}

// Credentials carries the MySQL credentials we resolved out of the
// mysql-credentials Secret.
type Credentials struct {
	RootUser     string
	RootPassword string
}

// LoadCredentials decodes the playground's mysql-credentials secret.
// The setup script writes the same secret on every fresh install, so
// callers can cache the result for the duration of a runner run.
func LoadCredentials(ctx context.Context, k *pgkube.Client, namespace string) (Credentials, error) {
	if namespace == "" {
		namespace = pgkube.PlaygroundNamespace
	}
	secret, err := k.Kubernetes.CoreV1().Secrets(namespace).Get(ctx, "mysql-credentials", metav1.GetOptions{})
	if err != nil {
		return Credentials{}, fmt.Errorf("get mysql-credentials: %w", err)
	}
	root, err := decodeKey(secret, "MYSQL_ROOT_PASSWORD")
	if err != nil {
		return Credentials{}, err
	}
	return Credentials{
		RootUser:     "root",
		RootPassword: root,
	}, nil
}

func decodeKey(secret *corev1.Secret, key string) (string, error) {
	if v, ok := secret.Data[key]; ok {
		return string(v), nil
	}
	if v, ok := secret.StringData[key]; ok {
		return v, nil
	}
	// Defensive: handle the case where the Secret was loaded as raw
	// base64 by a non-decoded path (e.g. stored as annotation).
	if v, ok := secret.Annotations[key]; ok {
		raw, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return "", fmt.Errorf("decode key %s: %w", key, err)
		}
		return strings.TrimSpace(string(raw)), nil
	}
	return "", fmt.Errorf("key %s not found in mysql-credentials secret", key)
}

// Open builds a SiteClient for the named site by port-forwarding its
// MySQL container on port 3306 and dialling the resulting local port
// with go-sql-driver/mysql.
func Open(ctx context.Context, k *pgkube.Client, namespace, fg, site string, creds Credentials) (*SiteClient, error) {
	if namespace == "" {
		namespace = pgkube.PlaygroundNamespace
	}
	pod, err := k.GetSiteMysqlPod(ctx, namespace, fg, site)
	if err != nil {
		return nil, err
	}
	pf, err := k.PortForwardPod(ctx, namespace, pod.Name, 3306)
	if err != nil {
		return nil, fmt.Errorf("port-forward mysql for site %s: %w", site, err)
	}
	dsn := fmt.Sprintf("%s:%s@tcp(127.0.0.1:%d)/?multiStatements=true&parseTime=true",
		creds.RootUser, creds.RootPassword, pf.LocalPort)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		pf.Stop()
		return nil, fmt.Errorf("open mysql for site %s: %w", site, err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		pf.Stop()
		return nil, fmt.Errorf("ping mysql for site %s: %w", site, err)
	}
	return &SiteClient{
		Site: site,
		DB:   db,
		pf:   pf,
	}, nil
}

// SuperReadOnly returns the value of @@super_read_only.
func (c *SiteClient) SuperReadOnly(ctx context.Context) (bool, error) {
	var v int
	if err := c.DB.QueryRowContext(ctx, "SELECT @@super_read_only").Scan(&v); err != nil {
		return false, fmt.Errorf("query super_read_only: %w", err)
	}
	return v == 1, nil
}

// ReadOnly returns the value of @@read_only.
func (c *SiteClient) ReadOnly(ctx context.Context) (bool, error) {
	var v int
	if err := c.DB.QueryRowContext(ctx, "SELECT @@read_only").Scan(&v); err != nil {
		return false, fmt.Errorf("query read_only: %w", err)
	}
	return v == 1, nil
}

// GtidExecuted returns @@gtid_executed.
func (c *SiteClient) GtidExecuted(ctx context.Context) (string, error) {
	var v string
	if err := c.DB.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&v); err != nil {
		return "", fmt.Errorf("query gtid_executed: %w", err)
	}
	return strings.TrimSpace(v), nil
}

// SetSuperReadOnly forces @@super_read_only=ON. Used to re-fence a
// site when a scenario needs to set up split-brain or self-fence
// preconditions.
func (c *SiteClient) SetSuperReadOnly(ctx context.Context, on bool) error {
	q := "SET GLOBAL super_read_only = ON"
	if !on {
		q = "SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF"
	}
	_, err := c.DB.ExecContext(ctx, q)
	return err
}

// ReplicaStatus is the small slice of SHOW REPLICA STATUS the runner
// needs for assertions. Empty struct means no replication configured.
type ReplicaStatus struct {
	Configured        bool
	IORunning         bool
	SQLRunning        bool
	SourceHost        string
	LastIOError       string
	LastSQLError      string
	ExecutedGtidSet   string
	RetrievedGtidSet  string
	SecondsBehindSrc  *int64
}

// ShowReplicaStatus issues SHOW REPLICA STATUS and returns the small
// subset the chaos runner asserts against.
func (c *SiteClient) ShowReplicaStatus(ctx context.Context) (ReplicaStatus, error) {
	rows, err := c.DB.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return ReplicaStatus{}, fmt.Errorf("show replica status: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return ReplicaStatus{Configured: false}, nil
	}
	cols, err := rows.Columns()
	if err != nil {
		return ReplicaStatus{}, fmt.Errorf("replica status columns: %w", err)
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return ReplicaStatus{}, fmt.Errorf("scan replica status: %w", err)
	}
	colMap := make(map[string]any, len(cols))
	for i, name := range cols {
		colMap[name] = vals[i]
	}
	rs := ReplicaStatus{Configured: true}
	rs.IORunning = asString(colMap["Replica_IO_Running"]) == "Yes"
	rs.SQLRunning = asString(colMap["Replica_SQL_Running"]) == "Yes"
	rs.SourceHost = asString(colMap["Source_Host"])
	rs.LastIOError = asString(colMap["Last_IO_Error"])
	rs.LastSQLError = asString(colMap["Last_SQL_Error"])
	rs.ExecutedGtidSet = strings.TrimSpace(asString(colMap["Executed_Gtid_Set"]))
	rs.RetrievedGtidSet = strings.TrimSpace(asString(colMap["Retrieved_Gtid_Set"]))
	if v, ok := colMap["Seconds_Behind_Source"]; ok && v != nil {
		if sbs, err := asInt64(v); err == nil {
			rs.SecondsBehindSrc = &sbs
		}
	}
	return rs, nil
}

// Exec runs a statement and returns the affected row count.
func (c *SiteClient) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := c.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ScalarInt runs a query and decodes the first column of the first
// row as an int64.
func (c *SiteClient) ScalarInt(ctx context.Context, query string, args ...any) (int64, error) {
	var v int64
	err := c.DB.QueryRowContext(ctx, query, args...).Scan(&v)
	return v, err
}

// ScalarString runs a query and decodes the first column of the
// first row as a string.
func (c *SiteClient) ScalarString(ctx context.Context, query string, args ...any) (string, error) {
	var v string
	err := c.DB.QueryRowContext(ctx, query, args...).Scan(&v)
	return v, err
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case []byte:
		return string(t)
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}

func asInt64(v any) (int64, error) {
	if v == nil {
		return 0, fmt.Errorf("nil value")
	}
	switch t := v.(type) {
	case []byte:
		var i int64
		_, err := fmt.Sscan(string(t), &i)
		return i, err
	case string:
		var i int64
		_, err := fmt.Sscan(t, &i)
		return i, err
	case int64:
		return t, nil
	case uint64:
		return int64(t), nil
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}
