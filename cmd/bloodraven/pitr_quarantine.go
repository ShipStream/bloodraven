package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func pitrQuarantine(ctx context.Context, args []string, out, errOut io.Writer) error {
	flags := flag.NewFlagSet("pitr-quarantine", flag.ContinueOnError)
	flags.SetOutput(errOut)
	site := flags.String("site", "", "Archive site")
	name := flags.String("name", "", "Exact binlog basename")
	id := flags.String("id", "", "Exact archive ID, or 'legacy' for a basename-only row")
	reason := flags.String("reason", "", "Audit reason; objects are preserved")
	resume := flags.String("resume-gtids", "", "Trusted canonical full-backup GTID bound for verification or affected-row exclusion")
	ack := flags.Bool("acknowledge-history", false, "Verify active history and overlapping issuance against a canonical backup, then supersede an obsolete guard")
	undo := flags.Bool("undo", false, "Restore a quarantined row after repairing its object")
	confirm := flags.Bool("confirm", false, "Apply the metadata change; stop the site's writer first")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(errOut, "Preserve a problematic object and quarantine only its manifest row.\nUsage: bloodraven pitr-quarantine --site SITE --name NAME --id ID|legacy --reason TEXT --resume-gtids SET --confirm\nOr: bloodraven pitr-quarantine --site SITE --acknowledge-history --reason TEXT --resume-gtids SET --confirm\nUses the same BLOODRAVEN_PITR_* storage configuration and mounted Secrets as pitr-download.\nStop the site's sidecar writer first; take a manifest copy and a canonical full backup. Use --undo to restore a repaired row.")
		flags.PrintDefaults()
	}
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			flags.Usage()
			return nil
		}
	}
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if *site == "" || !*confirm || flags.NArg() != 0 || (!*ack && (*name == "" || *id == "")) || (*ack && (*undo || *name != "" || *id != "")) {
		flags.Usage()
		return fmt.Errorf("site and explicit --confirm are required; choose name/id or --acknowledge-history")
	}
	if *id == "legacy" {
		*id = ""
	}
	cfg, err := pitrDownloadConfigFromEnv()
	if err != nil {
		return err
	}
	prefix := os.Getenv("BLOODRAVEN_PITR_MANIFEST_PREFIX")
	if prefix == "" {
		return fmt.Errorf("BLOODRAVEN_PITR_MANIFEST_PREFIX is required")
	}
	store, err := sidecar.NewArchiveStore(ctx, cfg)
	if err != nil {
		return err
	}
	if *ack {
		err = sidecar.AcknowledgeArchiveHistory(ctx, store, prefix, *site, *reason, *resume)
	} else {
		err = sidecar.QuarantineArchive(ctx, store, prefix, *site, *name, *id, *reason, *resume, *undo)
	}
	if err != nil {
		return err
	}
	if *ack {
		_, err = fmt.Fprintf(out, "Acknowledged history for site %s; earlier windows retain normal replay validation\n", *site)
	} else {
		_, err = fmt.Fprintf(out, "Updated %s/%s; archive objects preserved\n", *site, *name)
	}
	return err
}
