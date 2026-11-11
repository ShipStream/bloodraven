//go:build linux

package main

import "syscall"

// The f_type of the filesystems whose files are memory.
const (
	tmpfsMagic = 0x01021994
	ramfsMagic = 0x858458f6
)

// stagingInMemory reports whether dir is on tmpfs or ramfs (an emptyDir
// with medium Memory). A filesystem that cannot be examined counts as
// memory: spilling is then left off.
func stagingInMemory(dir string) bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return true
	}
	// f_type is signed and of varying width across architectures.
	switch uint32(st.Type) { //nolint:gosec // a magic number, compared bit for bit
	case tmpfsMagic, ramfsMagic:
		return true
	}
	return false
}
