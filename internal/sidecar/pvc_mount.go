package sidecar

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// procSelfMountInfo lists the mounts visible to this process. Kubelet
// volumes (PVCs, subPath bind mounts, emptyDirs) each appear here at
// their container mount point.
const procSelfMountInfo = "/proc/self/mountinfo"

// verifyPVCMountPoint fails unless path is itself a mount point in this
// process's mount namespace. A directory on the container's root
// filesystem accepts writes just as well, but loses them with the
// container, so the PVC archive store refuses to start on one.
func verifyPVCMountPoint(path string) error {
	f, err := os.Open(procSelfMountInfo)
	if err != nil {
		return fmt.Errorf("pvc archive store: cannot verify mount: %w", err)
	}
	defer f.Close()
	return checkMountPoint(path, f)
}

// TempDirMountVerifierForTest accepts directories under the system
// temporary directory as mounted volumes and rejects everything else.
// Tests set it as PITRPVCConfig.MountVerifier so PVC stores can live in
// t.TempDir(); production code never sets a verifier.
func TempDirMountVerifierForTest(path string) error {
	tmp, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if !pathWithin(tmp, resolved) || resolved == tmp {
		return fmt.Errorf("%s is not a mount point (test verifier accepts only %s subdirectories)", path, tmp)
	}
	return nil
}

// checkMountPoint resolves path and reports whether mountinfo lists it
// as a mount point. A subdirectory of a mount is rejected: the operator
// mounts the archive volume exactly at the configured path, so anything
// else means the volume is missing.
func checkMountPoint(path string, mountinfo io.Reader) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("pvc archive store: mount path %q is not absolute", path)
	}
	resolved, err := resolveExistingDir(filepath.Clean(path))
	if err != nil {
		return err
	}
	if resolved == "/" {
		return fmt.Errorf("pvc archive store: mount path %s resolves to the root filesystem", path)
	}
	mounts, err := parseMountInfo(mountinfo)
	if err != nil {
		return fmt.Errorf("pvc archive store: cannot verify mount: %w", err)
	}
	var parent *mountInfoEntry
	for i := range mounts {
		m := &mounts[i]
		if m.mountPoint == resolved {
			return nil
		}
		if pathWithin(m.mountPoint, resolved) && (parent == nil || len(m.mountPoint) > len(parent.mountPoint)) {
			parent = m
		}
	}
	if parent == nil {
		return fmt.Errorf("pvc archive store: %s is not a mount point; the archive volume is not mounted", path)
	}
	return fmt.Errorf("pvc archive store: %s is not a mount point (it is inside %s, fstype %s); the archive volume is not mounted",
		path, parent.mountPoint, parent.fsType)
}

func resolveExistingDir(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("pvc archive store: mount path %s does not exist", path)
		}
		return "", fmt.Errorf("pvc archive store: resolve mount path %s: %w", path, err)
	}
	fi, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("pvc archive store: stat mount path %s: %w", path, err)
	}
	if !fi.IsDir() {
		return "", fmt.Errorf("pvc archive store: mount path %s is not a directory", path)
	}
	return resolved, nil
}

// pathWithin reports whether p equals base or lies below it. Both must
// be clean absolute paths.
func pathWithin(base, p string) bool {
	if base == "/" {
		return strings.HasPrefix(p, "/")
	}
	return p == base || strings.HasPrefix(p, base+"/")
}

type mountInfoEntry struct {
	mountPoint string
	fsType     string
}

// parseMountInfo reads proc(5) mountinfo lines:
//
//	36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw
//
// Field 5 is the mount point. Optional fields run until the "-"
// separator, which is followed by the filesystem type.
func parseMountInfo(r io.Reader) ([]mountInfoEntry, error) {
	var out []mountInfoEntry
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Fields(line)
		sep := -1
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				sep = i
				break
			}
		}
		if len(fields) < 7 || sep < 0 || sep+1 >= len(fields) {
			return nil, fmt.Errorf("malformed mountinfo line %q", line)
		}
		out = append(out, mountInfoEntry{
			mountPoint: unescapeMountInfo(fields[4]),
			fsType:     unescapeMountInfo(fields[sep+1]),
		})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("mountinfo lists no mounts")
	}
	return out, nil
}

// unescapeMountInfo decodes the kernel's octal escapes (\040 space,
// \011 tab, \012 newline, \134 backslash).
func unescapeMountInfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) && isOctal(s[i+1]) && isOctal(s[i+2]) && isOctal(s[i+3]) {
			b.WriteByte((s[i+1]-'0')<<6 | (s[i+2]-'0')<<3 | (s[i+3] - '0'))
			i += 3
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func isOctal(c byte) bool { return c >= '0' && c <= '7' }

// probePVCWritable proves the store can create objects under prefix by
// writing a probe file in the deepest directory of root/prefix that
// already exists. Probing there rather than at root keeps shared volumes
// working whose root is not writable but whose prefix directory is. The
// probe ends in ".part", which List skips, so one left behind by a crash
// never becomes an archive object.
func probePVCWritable(root, prefix string) error {
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("pvc archive store: resolve %s: %w", root, err)
	}
	dir := filepath.Clean(root)
	if prefix != "" {
		clean := filepath.Clean(strings.TrimLeft(prefix, "/\\"))
		if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("pvc archive store: write prefix %q escapes %s", prefix, root)
		}
		if clean != "." {
			for _, part := range strings.Split(clean, string(filepath.Separator)) {
				next := filepath.Join(dir, part)
				fi, err := os.Lstat(next)
				if errors.Is(err, fs.ErrNotExist) {
					break
				}
				if err != nil {
					return fmt.Errorf("pvc archive store: stat %s: %w", next, err)
				}
				if fi.Mode()&fs.ModeSymlink == 0 && !fi.IsDir() {
					return fmt.Errorf("pvc archive store: %s is not a directory", next)
				}
				if fi.Mode()&fs.ModeSymlink != 0 {
					target, err := os.Stat(next)
					if err != nil {
						return fmt.Errorf("pvc archive store: resolve %s: %w", next, err)
					}
					if !target.IsDir() {
						return fmt.Errorf("pvc archive store: %s is not a directory", next)
					}
				}
				dir = next
			}
		}
	}
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return fmt.Errorf("pvc archive store: resolve %s: %w", dir, err)
	}
	if !pathWithin(resolvedRoot, resolvedDir) {
		return fmt.Errorf("pvc archive store: %s resolves to %s, outside the archive volume %s", dir, resolvedDir, resolvedRoot)
	}
	// Errors carry the directory, never the random probe name, so a
	// persistent failure reads the same on every retry (the supervisor's
	// log level and the operator's condition message key on the text).
	f, err := os.CreateTemp(resolvedDir, ".bloodraven-write-probe-*.part")
	if err != nil {
		return fmt.Errorf("pvc archive store: %s is not writable: %w", dir, withoutPath(err))
	}
	name := f.Name()
	_, writeErr := f.Write([]byte{0})
	closeErr := f.Close()
	removeErr := os.Remove(name)
	if err := errors.Join(withoutPath(writeErr), withoutPath(closeErr)); err != nil {
		return fmt.Errorf("pvc archive store: %s is not writable: %w", dir, err)
	}
	if removeErr != nil {
		return fmt.Errorf("pvc archive store: remove write probe in %s: %w", dir, withoutPath(removeErr))
	}
	return nil
}

// withoutPath strips the *fs.PathError wrapper, whose path is the random
// probe file name.
func withoutPath(err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	return err
}
