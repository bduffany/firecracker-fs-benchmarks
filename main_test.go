package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	crand "crypto/rand"
)

const (
	nFiles      = 100
	maxFileSize = 200_000_000
	nDirs       = 25
	maxDepth    = 8
)

func setup(b *testing.B) (dataDir, imgPath string) {
	// Generate disk image
	if _, err := os.Stat("gen"); err == nil {
		// gendir already exists
	} else if os.IsNotExist(err) {
		genDiskImage(b, "gen")
	}
	imgPath = filepath.Join("gen", "image.ext4")

	// Generate data dir
	var err error
	dataDir, err = os.MkdirTemp(".", "data-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dataDir) })

	// Return path to image, and path at which we want to unpack
	b.ResetTimer()
	return
}

func BenchmarkCopyOutputsToWorkspace_ExtractImage(b *testing.B) {
	dataDir, imgPath := setup(b)

	for i := 0; i < b.N; i++ {
		outDir := filepath.Join(dataDir, fmt.Sprintf("out_%d", i))
		if err := os.Mkdir(outDir, 0755); err != nil {
			b.Fatal(err)
		}
		if err := copyOutputsToWorkspace(context.Background(), false, imgPath, outDir); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCopyOutputsToWorkspace_MountImage(b *testing.B) {
	dataDir, imgPath := setup(b)

	for i := 0; i < b.N; i++ {
		outDir := filepath.Join(dataDir, fmt.Sprintf("out_%d", i))
		if err := os.Mkdir(outDir, 0755); err != nil {
			b.Fatal(err)
		}
		if err := copyOutputsToWorkspace(context.Background(), true, imgPath, outDir); err != nil {
			b.Fatal(err)
		}
	}
}

func genDiskImage(b *testing.B, genDir string) {
	fmt.Println("generating disk image")
	defer func() { fmt.Println("Done generating disk image.") }()

	if err := os.Mkdir(genDir, 0755); err != nil {
		b.Fatal(err)
	}

	root := filepath.Join(genDir, "root")
	if err := os.Mkdir(root, 0755); err != nil {
		b.Fatal(err)
	}

	imageSize := int64(0)

	// Generate dirs
	dirs := []string{}
	for i := 0; i < nDirs; i++ {
		nSegments := int(rand.Float64() * maxDepth)
		path := []string{root}
		for j := 0; j < nSegments; j++ {
			path = append(path, "dir_"+RandomString(b, 8))
			imageSize += 8e3
		}
		dir := filepath.Join(path...)
		if err := os.MkdirAll(dir, 0755); err != nil {
			b.Fatal(err)
		}
		dirs = append(dirs, dir)
	}

	// Generate files under the generated dirs
	buf := make([]byte, maxFileSize)
	for i := 0; i < nFiles; i++ {
		size := int(math.Pow(10, rand.Float64()*math.Log10(maxFileSize)))
		// fmt.Println("Generating file of size", size)
		dir := dirs[rand.Intn(len(dirs))]
		f, err := os.Create(filepath.Join(dir, "file_"+RandomString(b, 8)+".txt"))
		if err != nil {
			b.Fatal(err)
		}
		defer f.Close()
		if _, err := crand.Read(buf[:size]); err != nil {
			b.Fatal(err)
		}
		if _, err := f.Write(buf[:size]); err != nil {
			b.Fatal(err)
		}
		imageSize += 8e3 + int64(size)
		fmt.Println("Wrote", size, "bytes")
	}

	// Make disk image
	fmt.Println("Running mke2fs...")
	imgPath := filepath.Join(genDir, "image.ext4")
	if err := DirectoryToImage(context.Background(), root, imgPath, imageSize+1e9); err != nil {
		b.Fatal(err)
	}
}

func RandomString(b *testing.B, stringLength int) string {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, stringLength)
	if _, err := crand.Read(bytes); err != nil {
		b.Fatal(err)
	}
	// Run through bytes; replacing each with the equivalent random char.
	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}
	return string(bytes)
}

func copyOutputsToWorkspace(ctx context.Context, mountWorkspaceFile bool, imgPath, outDir string) error {
	wsDir, err := os.MkdirTemp(outDir, "workspacefs-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(wsDir) // clean up

	copyFn := os.Rename
	if mountWorkspaceFile {
		m, err := mountExt4ImageUsingLoopDevice(imgPath, wsDir)
		if err != nil {
			return err
		}
		defer m.Unmount()
		copyFn = copyFile
	} else {
		if err := ImageToDirectory(ctx, imgPath, wsDir); err != nil {
			return err
		}
	}

	walkErr := fs.WalkDir(os.DirFS(wsDir), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Skip /lost+found dir
		if path == "lost+found" {
			return fs.SkipDir
		}
		targetLocation := filepath.Join(outDir, path)

		_, err = os.Stat(targetLocation)
		if err == nil {
			return nil // already exists
		} else if !os.IsNotExist(err) {
			return err
		}

		if d.IsDir() {
			return os.Mkdir(targetLocation, 0755)
		}
		return copyFn(filepath.Join(wsDir, path), targetLocation)
	})
	return walkErr
}

type loopMount struct {
	loopControlFD *os.File
	imageFD       *os.File
	loopDevIdx    int
	loopFD        *os.File
	mountDir      string
}

func (m *loopMount) Unmount() error {
	if m.mountDir != "" {
		if err := syscall.Unmount(m.mountDir, 0); err != nil {
			return err
		}
		m.mountDir = ""
	}
	if m.loopDevIdx >= 0 && m.loopControlFD != nil {
		err := unix.IoctlSetInt(int(m.loopControlFD.Fd()), unix.LOOP_CTL_REMOVE, m.loopDevIdx)
		if err != nil {
			return err
		}
		m.loopDevIdx = -1
	}
	if m.loopFD != nil {
		m.loopFD.Close()
		m.loopFD = nil
	}
	if m.imageFD != nil {
		m.imageFD.Close()
		m.imageFD = nil
	}
	if m.loopControlFD != nil {
		m.loopControlFD.Close()
		m.loopControlFD = nil
	}
	return nil
}

func mountExt4ImageUsingLoopDevice(imagePath string, mountTarget string) (lm *loopMount, retErr error) {
	loopControlFD, err := os.Open("/dev/loop-control")
	if err != nil {
		return nil, err
	}
	defer loopControlFD.Close()

	m := &loopMount{loopDevIdx: -1}
	defer func() {
		if retErr != nil {
			if err := m.Unmount(); err != nil {
				panic("Could not unmount: " + err.Error())
			}
		}
	}()

	imageFD, err := os.Open(imagePath)
	if err != nil {
		return nil, err
	}
	m.imageFD = imageFD

	loopDevIdx, err := unix.IoctlRetInt(int(loopControlFD.Fd()), unix.LOOP_CTL_GET_FREE)
	if err != nil {
		return nil, fmt.Errorf("could not allocate loop device: %s", err)
	}
	m.loopDevIdx = loopDevIdx

	loopDevicePath := fmt.Sprintf("/dev/loop%d", loopDevIdx)
	loopFD, err := os.OpenFile(loopDevicePath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	m.loopFD = loopFD

	if err := unix.IoctlSetInt(int(loopFD.Fd()), unix.LOOP_SET_FD, int(imageFD.Fd())); err != nil {
		return nil, fmt.Errorf("could not set loop device FD: %s", err)
	}

	if err := syscall.Mount(loopDevicePath, mountTarget, "ext4", unix.MS_RDONLY, "norecovery"); err != nil {
		return nil, err
	}
	m.mountDir = mountTarget
	return m, nil
}

func tree(path string) {
	b, err := exec.Command("tree", "-A", "-C", "--inodes", path).CombinedOutput()
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(string(b))
}

func copyFile(src, dst string) error {
	stat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if stat.Mode().IsRegular() {
		sf, err := os.Open(src)
		if err != nil {
			return err
		}
		defer sf.Close()
		df, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, stat.Mode())
		if err != nil {
			return err
		}
		defer df.Close()
		_, err = io.Copy(df, sf)
		return err
	}

	if stat.Mode()&fs.ModeSymlink != 0 {
		target, err := os.Readlink(src)
		if err != nil {
			return err
		}
		return os.Symlink(target, dst)
	}

	return fmt.Errorf("file %q with mode %x is not a regular file or symlink", src, stat.Mode())
}

// DirectoryToImage creates an ext4 image of the specified size from inputDir
// and writes it to outputFile.
func DirectoryToImage(ctx context.Context, inputDir, outputFile string, sizeBytes int64) error {
	args := []string{
		"/sbin/mke2fs",
		"-L", "''",
		"-N", "0",
		"-O", "^64bit",
		"-d", inputDir,
		"-m", "5",
		"-r", "1",
		"-t", "ext4",
		outputFile,
		fmt.Sprintf("%dK", sizeBytes/1e3),
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Println(string(out))
		return err
	}
	return nil
}

// ImageToDirectory unpacks an ext4 image into outputDir, which must be empty.
func ImageToDirectory(ctx context.Context, inputFile, outputDir string) error {
	empty, err := isDirEmpty(outputDir)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("non-empty dir")
	}
	args := []string{
		"/sbin/debugfs",
		inputFile,
		"-R",
		fmt.Sprintf("rdump \"/\" \"%s\"", outputDir),
	}
	if out, err := exec.CommandContext(ctx, args[0], args[1:]...).CombinedOutput(); err != nil {
		fmt.Println(out)
		return err
	}
	return nil
}

// isDirEmpty returns a bool indicating if a directory contains no files, or
// an error.
func isDirEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()
	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}
