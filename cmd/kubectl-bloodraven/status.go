package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const statusUsage = `Show health of one or every MysqlFailoverGroup in a namespace.

Usage:
  kubectl bloodraven status [group] [flags]

When a group name is given, prints a multi-line report; otherwise prints
a table of every MysqlFailoverGroup in the namespace.

Flags:
  --kubeconfig string   Path to kubeconfig
  --context string      Kubeconfig context to use
  --namespace, -n       Namespace (default: context namespace, or "default")
  --output, -o string   "table" (default), "wide", "json", or "yaml"
  --all-namespaces, -A  List groups across all namespaces (output=table only)
`

func runStatus(args []string, stdout, stderr *os.File) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cf            commonFlags
		allNamespaces bool
	)
	registerCommonFlags(fs, &cf)
	fs.BoolVar(&allNamespaces, "all-namespaces", false, "list groups in every namespace")
	fs.BoolVar(&allNamespaces, "A", false, "list groups in every namespace (short)")
	fs.Usage = func() { fmt.Fprint(stderr, statusUsage) }
	if err := fs.Parse(args); err != nil {
		return err
	}
	positional := fs.Args()
	if len(positional) > 1 {
		return fmt.Errorf("status takes at most one positional argument (group name); got %d", len(positional))
	}

	cl, ns, err := cf.newClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if len(positional) == 1 {
		var fg v1alpha1.MysqlFailoverGroup
		key := client.ObjectKey{Namespace: ns, Name: positional[0]}
		if err := cl.Get(ctx, key, &fg); err != nil {
			return fmt.Errorf("get failover group %s/%s: %w", ns, positional[0], err)
		}
		return printGroupStatus(stdout, &fg, cf.output)
	}

	var list v1alpha1.MysqlFailoverGroupList
	var listOpts []client.ListOption
	if !allNamespaces {
		listOpts = append(listOpts, client.InNamespace(ns))
	}
	if err := cl.List(ctx, &list, listOpts...); err != nil {
		return fmt.Errorf("list failover groups: %w", err)
	}
	sort.Slice(list.Items, func(i, j int) bool {
		if list.Items[i].Namespace != list.Items[j].Namespace {
			return list.Items[i].Namespace < list.Items[j].Namespace
		}
		return list.Items[i].Name < list.Items[j].Name
	})
	return printGroupList(stdout, list.Items, cf.output, allNamespaces)
}

// printGroupList renders the per-group summary table. Wide output adds
// columns for planned-failover state and last-failover age.
func printGroupList(out io.Writer, items []v1alpha1.MysqlFailoverGroup, format string, allNamespaces bool) error {
	switch format {
	case "json":
		return encodeJSON(out, items)
	case "yaml":
		return encodeYAML(out, items)
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	wide := format == "wide"
	header := []string{}
	if allNamespaces {
		header = append(header, "NAMESPACE")
	}
	header = append(header, "NAME", "ACTIVE", "READY", "SITES")
	if wide {
		header = append(header, "PLANNED", "LAST-FAILOVER", "RECOVERY", "AGE")
	} else {
		header = append(header, "AGE")
	}
	fmt.Fprintln(tw, strings.Join(header, "\t"))

	for i := range items {
		fg := &items[i]
		row := []string{}
		if allNamespaces {
			row = append(row, fg.Namespace)
		}
		row = append(row,
			fg.Name,
			emptyDash(fg.Status.ActiveSite),
			readyString(fg),
			siteCountSummary(fg),
		)
		if wide {
			row = append(row,
				plannedFailoverSummary(fg),
				lastFailoverAge(fg),
				recoverySummary(fg),
				age(fg.CreationTimestamp.Time),
			)
		} else {
			row = append(row, age(fg.CreationTimestamp.Time))
		}
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	return tw.Flush()
}

// printGroupStatus renders the rich per-group report. JSON/YAML simply
// emit the CR.
func printGroupStatus(out io.Writer, fg *v1alpha1.MysqlFailoverGroup, format string) error {
	switch format {
	case "json":
		return encodeJSON(out, fg)
	case "yaml":
		return encodeYAML(out, fg)
	}

	fmt.Fprintf(out, "MysqlFailoverGroup: %s/%s\n", fg.Namespace, fg.Name)
	fmt.Fprintf(out, "  Active site: %s\n", emptyDash(fg.Status.ActiveSite))
	fmt.Fprintf(out, "  Ready: %s\n", readyString(fg))
	if fg.Spec.DNS.Hostname != "" {
		fmt.Fprintf(out, "  DNS: %s (TTL %ds)\n", fg.Spec.DNS.Hostname, fg.Spec.DNS.TTL)
	}
	if fg.Status.LastFailover != nil {
		target := fg.Status.LastFailoverTarget
		if target == "" {
			target = "?"
		}
		fmt.Fprintf(out, "  Last failover: %s ago, target %s\n", age(fg.Status.LastFailover.Time), target)
	}
	if fg.Status.UpdatePhase != "" {
		fmt.Fprintf(out, "  Update phase: %s\n", fg.Status.UpdatePhase)
	}

	fmt.Fprintln(out)
	fmt.Fprintln(out, "Sites:")
	siteTw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(siteTw, "  NAME\tROLE\tZONE\tSTATE\tREPL\tLAG\tRECOVERY\tLAST-SEEN")
	statusByName := map[string]v1alpha1.SiteStatus{}
	for i := range fg.Status.Sites {
		statusByName[fg.Status.Sites[i].Name] = fg.Status.Sites[i]
	}
	for _, site := range fg.Spec.Sites {
		st := statusByName[site.Name]
		fmt.Fprintf(siteTw, "  %s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			site.Name,
			string(site.EffectiveRole()),
			site.Zone,
			emptyDash(st.State),
			boolYesNo(st.Replicating),
			lagString(st.SecondsBehindSource),
			emptyDash(st.RecoveryState),
			timeAge(st.LastSeen),
		)
	}
	if err := siteTw.Flush(); err != nil {
		return err
	}

	if fg.Status.PlannedFailover != nil {
		fmt.Fprintln(out)
		printPlannedFailover(out, fg.Status.PlannedFailover)
	}
	if fg.Status.Restore != nil {
		fmt.Fprintln(out)
		printRestore(out, fg.Status.Restore)
	}
	if fg.Status.RestoreInPlace != nil {
		fmt.Fprintln(out)
		printRestoreInPlace(out, fg.Status.RestoreInPlace)
	}
	if len(fg.Status.BackupSchedules) > 0 {
		fmt.Fprintln(out)
		printBackupSchedules(out, fg.Status.BackupSchedules)
	}
	if fg.Status.PITR != nil && fg.Status.PITR.Enabled {
		fmt.Fprintln(out)
		printPITRStatus(out, fg.Status.PITR)
	}
	if len(fg.Status.Conditions) > 0 {
		fmt.Fprintln(out)
		printConditions(out, fg.Status.Conditions)
	}
	return nil
}

func printPlannedFailover(out io.Writer, pf *v1alpha1.PlannedFailoverStatus) {
	fmt.Fprintln(out, "Planned failover:")
	fmt.Fprintf(out, "  Phase: %s\n", emptyDash(string(pf.Phase)))
	if pf.SourcePrimary != "" || pf.Target != "" {
		fmt.Fprintf(out, "  Source -> Target: %s -> %s\n", emptyDash(pf.SourcePrimary), emptyDash(pf.Target))
	}
	if pf.Reason != "" {
		fmt.Fprintf(out, "  Reason: %s\n", pf.Reason)
	}
	if pf.Message != "" {
		fmt.Fprintf(out, "  Message: %s\n", pf.Message)
	}
	if pf.StartTime != nil {
		fmt.Fprintf(out, "  Started: %s ago\n", age(pf.StartTime.Time))
	}
	if pf.CompletionTime != nil {
		fmt.Fprintf(out, "  Completed: %s ago\n", age(pf.CompletionTime.Time))
	}
	if pf.DurationSeconds != nil {
		fmt.Fprintf(out, "  Duration: %ds\n", *pf.DurationSeconds)
	}
	if pf.TransactionsLost != nil {
		fmt.Fprintf(out, "  Transactions lost: %d\n", *pf.TransactionsLost)
	}
	if pf.RetryAfter != nil {
		fmt.Fprintf(out, "  Retry after: %s\n", pf.RetryAfter.Time.UTC().Format(time.RFC3339))
	}
}

func printRestore(out io.Writer, r *v1alpha1.RestoreStatus) {
	fmt.Fprintln(out, "Initial restore (initFromBackup):")
	fmt.Fprintf(out, "  Phase: %s\n", emptyDash(string(r.Phase)))
	if r.TargetSite != "" {
		fmt.Fprintf(out, "  Target site: %s\n", r.TargetSite)
	}
	if r.Message != "" {
		fmt.Fprintf(out, "  Message: %s\n", r.Message)
	}
	if r.StartTime != nil {
		fmt.Fprintf(out, "  Started: %s ago\n", age(r.StartTime.Time))
	}
	if r.CompletionTime != nil {
		fmt.Fprintf(out, "  Completed: %s ago\n", age(r.CompletionTime.Time))
	}
	if r.SourceSizeBytes > 0 {
		fmt.Fprintf(out, "  Source size: %s\n", humanBytes(r.SourceSizeBytes))
	}
}

func printRestoreInPlace(out io.Writer, r *v1alpha1.RestoreInPlaceStatus) {
	fmt.Fprintln(out, "In-place restore:")
	fmt.Fprintf(out, "  Phase: %s\n", emptyDash(string(r.Phase)))
	if r.Scope != "" {
		fmt.Fprintf(out, "  Scope: %s\n", r.Scope)
	}
	if r.TargetSite != "" {
		fmt.Fprintf(out, "  Target site: %s\n", r.TargetSite)
	}
	if r.Message != "" {
		fmt.Fprintf(out, "  Message: %s\n", r.Message)
	}
	if r.StartTime != nil {
		fmt.Fprintf(out, "  Started: %s ago\n", age(r.StartTime.Time))
	}
	if r.CompletionTime != nil {
		fmt.Fprintf(out, "  Completed: %s ago\n", age(r.CompletionTime.Time))
	}
}

func printBackupSchedules(out io.Writer, ss []v1alpha1.BackupScheduleStatus) {
	fmt.Fprintln(out, "Backup schedules:")
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  NAME\tLAST-RUN\tLAST-OK\tLAST-PHASE\tLAST-BACKUP\tATTEMPT")
	for _, s := range ss {
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%d\n",
			s.Name,
			timeAge(s.LastScheduleTime),
			timeAge(s.LastSuccessfulTime),
			emptyDash(s.LastBackupPhase),
			emptyDash(s.LastBackupName),
			s.LastRetryAttempt,
		)
	}
	_ = tw.Flush()
}

func printPITRStatus(out io.Writer, p *v1alpha1.PITRStatus) {
	fmt.Fprintln(out, "PITR archive:")
	fmt.Fprintf(out, "  Profile: %s\n", p.ProfileName)
	if p.OldestArchivedTime != nil && p.NewestArchivedTime != nil {
		fmt.Fprintf(out, "  Window: %s -> %s (%s)\n",
			p.OldestArchivedTime.Time.UTC().Format(time.RFC3339),
			p.NewestArchivedTime.Time.UTC().Format(time.RFC3339),
			p.NewestArchivedTime.Time.Sub(p.OldestArchivedTime.Time).Round(time.Second),
		)
	}
	if p.ArchivedFileCount > 0 {
		fmt.Fprintf(out, "  Files: %d, %s on disk\n", p.ArchivedFileCount, humanBytes(p.ArchivedBytes))
	}
	if p.LastArchivedTime != nil {
		fmt.Fprintf(out, "  Last archived: %s ago\n", age(p.LastArchivedTime.Time))
	}
	if p.Message != "" {
		fmt.Fprintf(out, "  Message: %s\n", p.Message)
	}
}

func printConditions(out io.Writer, conds []metav1.Condition) {
	fmt.Fprintln(out, "Conditions:")
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  TYPE\tSTATUS\tREASON\tMESSAGE")
	for _, c := range conds {
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n",
			c.Type, string(c.Status), emptyDash(c.Reason), truncate(c.Message, 80))
	}
	_ = tw.Flush()
}
