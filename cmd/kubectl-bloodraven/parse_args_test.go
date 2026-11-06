package main

import (
	"flag"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestParseFlagsAndArgs(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		wantPos []string
		wantNS  string
		wantB   bool
	}{
		{[]string{"orders", "-n", "orders"}, []string{"orders"}, "orders", false},
		{[]string{"-n", "orders", "orders"}, []string{"orders"}, "orders", false},
		{[]string{"orders", "iad", "-n", "x", "--cold"}, []string{"orders", "iad"}, "x", true},
		{[]string{"orders", "--cold", "iad", "--namespace=x"}, []string{"orders", "iad"}, "x", true},
		{[]string{"orders", "--", "-n", "x"}, []string{"orders", "-n", "x"}, "", false},
		{nil, nil, "", false},
	} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		ns := fs.String("n", "", "")
		fs.StringVar(ns, "namespace", "", "")
		b := fs.Bool("cold", false, "")
		pos, err := parseFlagsAndArgs(fs, tc.args)
		if err != nil {
			t.Fatalf("%v: %v", tc.args, err)
		}
		if !slices.Equal(pos, tc.wantPos) || *ns != tc.wantNS || *b != tc.wantB {
			t.Errorf("%v: positional %q ns %q cold %v; want %q %q %v", tc.args, pos, *ns, *b, tc.wantPos, tc.wantNS, tc.wantB)
		}
	}
}

// TestCommandsAcceptTrailingFlags pins the documented command forms, which
// put flags after the positionals (G-m3). Each command must get past its
// positional-count check and fail only at loading the missing kubeconfig.
func TestCommandsAcceptTrailingFlags(t *testing.T) {
	kc := filepath.Join(t.TempDir(), "missing-kubeconfig")
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer devnull.Close()
	for _, args := range [][]string{
		{"ack-planned-failover-hold", "orders", "-n", "orders", "--kubeconfig", kc},
		{"ack-planned-failover-hold", "orders", "-n", "orders", "--run-id", "r1", "--kubeconfig", kc},
		{"promote", "orders", "pdx", "-n", "orders", "--kubeconfig", kc},
		{"reclone", "orders", "iad", "-n", "orders", "--cold", "--kubeconfig", kc},
		{"backup", "orders", "--profile", "nightly", "-n", "orders", "--kubeconfig", kc},
		{"verify-backup", "orders", "--profile", "nightly", "-n", "orders", "--kubeconfig", kc},
		{"status", "orders", "-n", "orders", "--kubeconfig", kc},
	} {
		err := run(args, devnull, devnull)
		if err == nil || strings.Contains(err.Error(), "positional") || !strings.Contains(err.Error(), "kubeconfig") {
			t.Errorf("run(%q) = %v; want a kubeconfig load error", args, err)
		}
	}
}
