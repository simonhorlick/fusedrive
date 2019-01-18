package main

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/simonhorlick/fusedrive/metadb"
	"log"
	"os"
	"syscall"
)


// FileReference is the fuse file that's backed by a local file.
type FileReference struct {
	file *os.File
	cache *LocalFileCache
	name string
	isReader bool

	db *metadb.DB

	// We embed a nodefs.NewDefaultFile() that returns ENOSYS for every
	// operation we have not implemented. This prevents build breakage when the
	// go-fuse library adds new methods to the nodefs.File interface.
	nodefs.File
}

func (f *FileReference) SetInode(*nodefs.Inode) {
}


func (f *FileReference) String() string {
	return fmt.Sprintf("FileReference(%s)", f.name)
}

func (f *FileReference) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	log.Printf("Read for %s at offset %d bufsize %d", f.name, off, len(buf))
	r := fuse.ReadResultFd(f.file.Fd(), off, len(buf))
	return r, fuse.OK
}

func (f *FileReference) Write(data []byte, off int64) (uint32, fuse.Status) {
	log.Printf("Write for %s at offset %d bufsize %d", f.name, off, len(data))

	if f.isReader {
		return 0, fuse.EPERM
	}

	f.cache.MarkDirty(f)
	n, err := f.file.WriteAt(data, off)
	return uint32(n), fuse.ToStatus(err)
}

func (f *FileReference) Flush() fuse.Status {
	log.Printf("Flush for %s", f.name)

	// Since Flush() may be called for each dup'd fd, we don't
	// want to really close the file, we just want to flush. This
	// is achieved by closing a dup'd fd.
	newFd, err := syscall.Dup(int(f.file.Fd()))

	if err != nil {
		return fuse.ToStatus(err)
	}
	err = syscall.Close(newFd)
	return fuse.ToStatus(err)
}

func (f *FileReference) Fsync(flags int) (code fuse.Status) {
	log.Printf("Fsync for %s", f.name)
	r := fuse.ToStatus(syscall.Fsync(int(f.file.Fd())))
	return r
}

func (f *FileReference) Truncate(size uint64) fuse.Status {
	log.Printf("Truncate for %s", f.name)
	r := fuse.ToStatus(syscall.Ftruncate(int(f.file.Fd()), int64(size)))
	return r
}

func (f *FileReference) Chmod(mode uint32) fuse.Status {
	log.Printf("Chmod for %s", f.name)
	err := f.db.SetMode(f.name, mode)
	if err != nil {
		return fuse.EPERM
	}
	return fuse.OK
}

func (f *FileReference) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.ENOSYS
}

func (f *FileReference) GetAttr(out *fuse.Attr) fuse.Status {
	log.Printf("GetAttr for %s", f.name)

	attributes, err := f.db.GetAttributes(f.name)

	if err == metadb.DoesNotExist {
		return fuse.ENOENT
	} else if err != nil {
		log.Printf("failed to read file metadata %s: %v", f.name, err)
		return fuse.ENODATA
	}

	toFuseAttributes(attributes, out)

	return fuse.OK
}
