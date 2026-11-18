package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	sigyaml "sigs.k8s.io/yaml"
)

const workloadMonitorPassword = "monitor-native-$password"

func TestMySQLWorkloadNative(t *testing.T) {
	if os.Getenv("BLOODRAVEN_MYSQL_NATIVE_TEST") != "1" {
		t.Skip("opt-in MySQL workload dashboard test (BLOODRAVEN_MYSQL_NATIVE_TEST=1, docker)")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal("docker is required")
	}
	env := newLagNativeEnv(t, docker, defaultMySQLImage)
	iad, pdx := env.start("iad", 101), env.start("pdx", 102)
	iad.waitUp()
	pdx.waitUp()

	monitor := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "warehouse", Name: "orders-mysql-monitor"},
		Data:       map[string][]byte{"username": []byte("monitor"), "password": []byte(workloadMonitorPassword)},
	}
	reconciler := MysqlFailoverGroupReconciler{Client: fake.NewClientBuilder().WithObjects(monitor).Build()}
	fg := &v1beta1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: monitor.Namespace, Name: "orders"}}
	fg.Spec.Credentials.MonitorSecretName = monitor.Name
	roles := reconciler.buildRoles(t.Context(), fg)
	if len(roles) != 1 || roles[0].name != "monitor" {
		t.Fatalf("expected configured monitor account, got %d roles", len(roles))
	}

	for i, site := range []*lagNativeServer{iad, pdx} {
		site.session("SET sql_log_bin=0",
			"CREATE USER 'repl'@'%' IDENTIFIED BY '"+lagNativePassword+"'",
			"GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%'",
			"SET sql_log_bin=1")
		if err := reconcileRole(t.Context(), site.db, roles[0]); err != nil {
			t.Fatalf("provision monitor account for %s: %v", site.alias, err)
		}
		site.exec(fmt.Sprintf("SET GLOBAL max_connections=%d", 80+i*40))
		site.exec("RESET BINARY LOGS AND GTIDS")
	}
	iad.exec("CREATE DATABASE workload")
	iad.exec("CREATE TABLE workload.counter (id INT PRIMARY KEY, n INT NOT NULL) ENGINE=InnoDB")
	iad.exec("INSERT INTO workload.counter VALUES (1, 0)")
	pdx.exec("SET GLOBAL super_read_only=ON")
	pdx.replicateFrom(iad, 0)
	workloadWaitGTID(t, pdx, iad)

	exporter := startWorkloadExporter(t, env)
	prometheus := startWorkloadPrometheus(t, env, exporter, iad.alias, pdx.alias)
	queries := readWorkloadQueries(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	assertPanel := func(metric, site string, compare func(float64) bool) {
		t.Helper()
		query := queries[metric]
		if query == "" {
			t.Fatalf("dashboard has no query using %s", metric)
		}
		lagNativeWait(t, metric+" on "+site, 12*time.Second, func() bool {
			values, ok := workloadQuery(ctx, prometheus, query)
			value, present := values[site]
			return ok && present && compare(value)
		})
	}
	locks := "mysql_global_status_innodb_row_lock_current_waits"
	waitRate := "mysql_global_status_innodb_row_lock_waits"
	throughput := "mysql_global_status_questions"
	utilization := "mysql_global_variables_max_connections"
	zero := func(value float64) bool { return value == 0 }
	positive := func(value float64) bool { return value > 0 }
	loaded := func(value float64) bool { return value > 10 }

	workload := func(site *lagNativeServer, name string) {
		t.Helper()
		assertPanel(locks, name, zero)
		assertPanel(utilization, name, func(v float64) bool { return v > 0 && v <= 100 })
		t.Logf("%s: idle locks measured zero and connections are below their limit", name)
		tx, err := site.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, "UPDATE workload.counter SET n=n+1 WHERE id=1"); err != nil {
			t.Fatal(err)
		}
		waiter := make(chan error, 1)
		go func() {
			_, err := site.db.ExecContext(ctx, "UPDATE workload.counter SET n=n+1 WHERE id=1")
			waiter <- err
		}()
		assertPanel(locks, name, positive)
		assertPanel(waitRate, name, positive)
		for range 120 {
			site.exec("SELECT 1")
		}
		assertPanel(throughput, name, loaded)
		t.Logf("%s: InnoDB wait detected and client SQL throughput exceeded 10 queries/s", name)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-waiter:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		assertPanel(locks, name, zero)
	}

	workload(iad, "iad")
	workloadWaitGTID(t, pdx, iad)
	if err := iad.checker.SetReadOnly(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := pdx.checker.Promote(ctx); err != nil {
		t.Fatal(err)
	}
	iad.replicateFrom(pdx, 0)
	t.Log("MySQL handoff from iad to pdx complete; scrape target names unchanged")
	workload(pdx, "pdx")
	workloadWaitGTID(t, iad, pdx)
	assertPanel("configured limit", "iad", func(v float64) bool { return v == 80 })
	assertPanel("configured limit", "pdx", func(v float64) bool { return v == 120 })
	assertPanel(utilization, "iad", positive)
	assertPanel(utilization, "pdx", positive)

	// The scrape endpoint remains HTTP-up after the database disappears.
	// A real zero must disappear from the panel as soon as its source fails.
	workloadDocker(t, env, "stop", iad.name)
	lagNativeWait(t, "MySQL down with exporter healthy", 12*time.Second, func() bool {
		mysql, mysqlOK := workloadQuery(ctx, prometheus, `mysql_up{job="bloodraven-mysql"}`)
		up, upOK := workloadQuery(ctx, prometheus, `up{job="bloodraven-mysql"}`)
		value, exists := mysql["iad"]
		return mysqlOK && upOK && exists && value == 0 && up["iad"] == 1
	})
	for _, metric := range []string{locks, waitRate, throughput, utilization} {
		lagNativeWait(t, metric+" missing for iad only", 12*time.Second, func() bool {
			values, ok := workloadQuery(ctx, prometheus, queries[metric])
			_, iadPresent := values["iad"]
			_, pdxPresent := values["pdx"]
			return ok && !iadPresent && pdxPresent
		})
	}
}

func workloadWaitGTID(t *testing.T, replica, source *lagNativeServer) {
	t.Helper()
	set := source.query("SELECT @@GLOBAL.gtid_executed")
	if got := replica.query(fmt.Sprintf("SELECT WAIT_FOR_EXECUTED_GTID_SET('%s', 15)", set)); got != "0" {
		t.Fatalf("site %s did not apply the source's GTIDs: %q", replica.alias, got)
	}
}

func workloadDocker(t *testing.T, env *lagNativeEnv, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, env.docker, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", args[0], err, out)
	}
	return strings.TrimSpace(string(out))
}

func startWorkloadContainer(t *testing.T, env *lagNativeEnv, service string, args ...string) string {
	t.Helper()
	name := env.network + "-" + service
	flags := []string{"run", "-d", "--name", name, "--network", env.network, "--network-alias", service}
	workloadDocker(t, env, append(flags, args...)...)
	t.Cleanup(func() {
		if t.Failed() {
			if logs, err := exec.Command(env.docker, "logs", "--tail", "25", name).CombinedOutput(); err == nil {
				t.Logf("%s log:\n%s", service, logs)
			}
		}
		_ = exec.Command(env.docker, "rm", "-fv", name).Run()
	})
	return name
}

func workloadFile(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func startWorkloadExporter(t *testing.T, env *lagNativeEnv) string {
	t.Helper()
	raw, err := os.ReadFile("../../examples/mysqld-exporter.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var deployment appsv1.Deployment
	var auth corev1.ConfigMap
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
	for {
		var object json.RawMessage
		if err := decoder.Decode(&object); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		var meta corev1.ObjectReference
		if err := json.Unmarshal(object, &meta); err != nil {
			t.Fatal(err)
		}
		switch meta.Kind {
		case "Deployment":
			err = json.Unmarshal(object, &deployment)
		case "ConfigMap":
			err = json.Unmarshal(object, &auth)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	containers := deployment.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatal("expected one exporter container")
	}
	container := containers[0]
	var cnfPath string
	for _, arg := range container.Args {
		if path, found := strings.CutPrefix(arg, "--config.my-cnf="); found {
			cnfPath = path
		}
	}
	cnf, ok := auth.Data[filepath.Base(cnfPath)]
	if !ok {
		t.Fatal("exporter credential template missing")
	}
	flags := []string{"-v", workloadFile(t, "my.cnf", cnf) + ":" + cnfPath + ":ro"}
	for _, variable := range container.Env {
		if variable.ValueFrom == nil || variable.ValueFrom.SecretKeyRef == nil {
			t.Fatalf("expected a monitor Secret reference for %s", variable.Name)
		}
		switch variable.ValueFrom.SecretKeyRef.Key {
		case "username":
			flags = append(flags, "-e", variable.Name+"=monitor")
		case "password":
			flags = append(flags, "-e", variable.Name+"="+workloadMonitorPassword)
		default:
			t.Fatalf("unexpected monitor Secret key for %s", variable.Name)
		}
	}
	const alias = "exporter"
	startWorkloadContainer(t, env, alias, append(append(flags, container.Image), container.Args...)...)
	return alias + ":9104"
}

func startWorkloadPrometheus(t *testing.T, env *lagNativeEnv, exporter, iad, pdx string) string {
	t.Helper()
	config, err := os.ReadFile("../../examples/mysqld-exporter-scrape.yaml")
	if err != nil {
		t.Fatal(err)
	}
	patched := strings.NewReplacer(
		"orders-mysqld-exporter.warehouse.svc.cluster.local:9104", exporter,
		"mysql-orders-iad-internal.warehouse.svc.cluster.local:3306", iad+":3306",
		"mysql-orders-pdx-internal.warehouse.svc.cluster.local:3306", pdx+":3306",
	).Replace(string(config))
	var document struct {
		Jobs []map[string]any `json:"scrape_configs"`
	}
	if err := sigyaml.Unmarshal([]byte(patched), &document); err != nil {
		t.Fatal(err)
	}
	if len(document.Jobs) != 1 || document.Jobs[0]["job_name"] != "bloodraven-mysql-warehouse-orders" {
		t.Fatal("expected the documented workload scrape job")
	}
	document.Jobs[0]["scrape_interval"] = "1s"
	document.Jobs[0]["scrape_timeout"] = "1s"
	raw, err := sigyaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	name := startWorkloadContainer(t, env, "prometheus", "-p", "127.0.0.1::9090",
		"-v", workloadFile(t, "prometheus.yml", string(raw))+":/etc/prometheus/prometheus.yml:ro",
		"prom/prometheus:v3.10.0")
	address := workloadDocker(t, env, "port", name, "9090/tcp")
	return "http://" + address + "/api/v1/query"
}

func readWorkloadQueries(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile("../../charts/bloodraven/dashboards/mysql-workload.json")
	if err != nil {
		t.Fatal(err)
	}
	var dashboard struct {
		Panels []struct {
			Type    string
			Targets []struct{ Expr string }
		}
	}
	if err := json.Unmarshal(raw, &dashboard); err != nil {
		t.Fatal(err)
	}
	replace := strings.NewReplacer("${ns:regex}", "warehouse", "${group:regex}", "orders", "${site:regex}", "iad|pdx",
		"$ns", "warehouse", "$group", "orders", "$site", "iad|pdx", "$__rate_interval", "8s")
	queries := map[string]string{}
	for _, panel := range dashboard.Panels {
		for _, target := range panel.Targets {
			for _, metric := range []string{"mysql_global_variables_max_connections", "mysql_global_status_innodb_row_lock_current_waits",
				"mysql_global_status_innodb_row_lock_waits", "mysql_global_status_questions"} {
				if strings.Contains(target.Expr, metric+"{") && (panel.Type == "timeseries" || metric == "mysql_global_status_innodb_row_lock_current_waits") {
					key := metric
					if metric == "mysql_global_variables_max_connections" && !strings.Contains(target.Expr, "mysql_global_status_threads_connected") {
						key = "configured limit"
					}
					queries[key] = replace.Replace(target.Expr)
				}
			}
		}
	}
	return queries
}

func workloadQuery(ctx context.Context, endpoint, query string) (map[string]float64, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?query="+url.QueryEscape(query), nil)
	if err != nil {
		return nil, false
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false
	}
	defer func() { _ = res.Body.Close() }()
	var payload struct {
		Status string
		Data   struct {
			Result []struct {
				Metric map[string]string
				Value  []json.RawMessage
			}
		}
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil || payload.Status != "success" {
		return nil, false
	}
	values := map[string]float64{}
	for _, sample := range payload.Data.Result {
		site := sample.Metric["site"]
		var raw string
		if len(sample.Value) != 2 || sample.Metric["group"] != "orders" || sample.Metric["namespace"] != "warehouse" ||
			json.Unmarshal(sample.Value[1], &raw) != nil {
			return nil, false
		}
		value, err := strconv.ParseFloat(raw, 64)
		if _, duplicate := values[site]; err != nil || duplicate || site == "" {
			return nil, false
		}
		values[site] = value
	}
	return values, true
}
