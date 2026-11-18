package main

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/shipstream/bloodraven/internal/crdcompat"
)

func TestReportCRDCheck(t *testing.T) {
	older := crdcompat.Finding{Subject: "mysqlfailovergroups.shipstream.io", Detail: "the installed CustomResourceDefinition is older than this operator needs: status.rollout is missing"}
	newer := crdcompat.Finding{Subject: "mysqldatabases.shipstream.io", Detail: "the installed CustomResourceDefinition is newer than this operator: spec.futureField is not known to this build"}
	rule := crdcompat.Finding{Subject: "MysqlBackup orders/nightly", Detail: "the stored object breaks a validation rule of the installed schema: spec.profileName: Too long"}

	for _, tc := range []struct {
		name      string
		report    crdcompat.StartupReport
		mode      string
		wantErr   string
		wantNewer string
		wantLogs  []string
		noLogs    []string
	}{
		{name: "clean", report: crdcompat.StartupReport{Checked: map[string]int{"MysqlFailoverGroup": 2, "MysqlBackup": 5}}, mode: crdCheckEnforce,
			wantLogs: []string{msgCRDCheckPassed, "objects=7"}, noLogs: []string{"level=WARN", "level=ERROR"}},
		{name: "a newer CRD and a broken rule only warn", report: crdcompat.StartupReport{NewerCRDs: []crdcompat.Finding{newer}, RuleViolations: []crdcompat.Finding{rule}}, mode: crdCheckEnforce,
			wantNewer: newer.String(),
			wantLogs:  []string{msgCRDCheckNewerCRD, "spec.futureField", msgCRDCheckRuleViolation, "MysqlBackup orders/nightly", msgCRDCheckPassed},
			noLogs:    []string{"level=ERROR"}},
		{name: "an older CRD refuses", report: crdcompat.StartupReport{Refusals: []crdcompat.Finding{older}, NewerCRDs: []crdcompat.Finding{newer}}, mode: crdCheckEnforce,
			wantErr:  "1 problem(s) with the installed CRDs or the stored objects, the first: mysqlfailovergroups.shipstream.io: the installed CustomResourceDefinition is older",
			wantLogs: []string{"level=ERROR", msgCRDCheckRefused, "status.rollout is missing", crdCheckDocs}, noLogs: []string{msgCRDCheckPassed}},
		{name: "the break-glass starts anyway and says so", report: crdcompat.StartupReport{Refusals: []crdcompat.Finding{older}}, mode: crdCheckWarn,
			wantLogs: []string{"level=ERROR", msgCRDCheckRefused, msgCRDCheckOverridden}, noLogs: []string{msgCRDCheckPassed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			newerCRDs, err := reportCRDCheck(tc.report, tc.mode, slog.New(slog.NewTextHandler(&logs, nil)))
			if (err == nil) != (tc.wantErr == "") || (err != nil && !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
			if newerCRDs != tc.wantNewer {
				t.Fatalf("newer CRDs = %q, want %q", newerCRDs, tc.wantNewer)
			}
			for _, want := range tc.wantLogs {
				if !strings.Contains(logs.String(), want) {
					t.Errorf("the log lacks %q:\n%s", want, logs.String())
				}
			}
			for _, not := range tc.noLogs {
				if strings.Contains(logs.String(), not) {
					t.Errorf("the log has %q:\n%s", not, logs.String())
				}
			}
		})
	}
}

func TestRunCRDCheckRejectsAnUnknownMode(t *testing.T) {
	if _, err := runCRDCheck(t.Context(), nil, "off", slog.New(slog.DiscardHandler)); err == nil || !strings.Contains(err.Error(), `want "enforce" or "warn"`) {
		t.Fatalf("mode off = %v, want it refused: there is no way to switch the check off silently", err)
	}
}

func TestCRDCheckErrorNamesAMissingPermission(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"},
		"mysqlfailovergroups.shipstream.io", errors.New("RBAC: clusterrole lacks the rule"))
	wrapped := fmt.Errorf("read CustomResourceDefinition mysqlfailovergroups.shipstream.io: %w", forbidden)
	if !isForbidden(wrapped) {
		t.Fatal("a wrapped Forbidden is not recognised")
	}
	if msg := crdCheckError(wrapped).Error(); !strings.Contains(msg, "ClusterRole lacks a permission") || !strings.Contains(msg, "upgrade the RBAC with the operator") {
		t.Fatalf("message = %q, want it to say the role must be upgraded", msg)
	}
	other := crdCheckError(errors.New("connection refused"))
	if isForbidden(other) || strings.Contains(other.Error(), "ClusterRole") {
		t.Fatalf("an unreachable API server is reported as a permission problem: %v", other)
	}
}
