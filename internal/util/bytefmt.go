package util

import "fmt"

// HumanBytes formats a byte count using binary (1024-based) units so
// the Go side produces the exact same string the Python backup
// scripts write into the BLOODRAVEN_DUMP_COMPLETE sentinel. Values
// below 1 KiB are returned as "<N> B"; everything else gets one
// decimal and a unit suffix from {KiB, MiB, GiB, TiB, PiB, EiB}.
//
// Shared so the reconciler's .status.size string matches the one the
// encrypt-upload subcommand emits — previously two copies (AUDIT L8).
func HumanBytes(n int64) string {
	const unit = int64(1024)
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := unit, 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	suffix := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	return fmt.Sprintf("%.1f %s", float64(n)/float64(div), suffix[exp])
}
