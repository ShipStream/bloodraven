package main

import (
	"flag"
	"fmt"
	"io"
	"runtime/debug"
)

// Version is the plugin version string. Stamped by the Makefile via
// -ldflags when built from a release tag; defaults to "dev" otherwise.
var Version = "dev"

func runVersion(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	fs.SetOutput(stdout)
	short := fs.Bool("short", false, "print only the version string")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *short {
		fmt.Fprintln(stdout, Version)
		return nil
	}
	fmt.Fprintf(stdout, "kubectl-bloodraven %s\n", Version)
	if info, ok := debug.ReadBuildInfo(); ok {
		fmt.Fprintf(stdout, "  go: %s\n", info.GoVersion)
		fmt.Fprintf(stdout, "  module: %s\n", info.Main.Path)
	}
	return nil
}
