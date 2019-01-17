package main

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/simonhorlick/fusedrive/api"
	"github.com/simonhorlick/fusedrive/metadb"
	"log"
	"os"
	"sync"
	"syscall"
)

// WritableFile delegates all operations back to an underlying os.File.
func NewWritableFile(f *os.File, id, name string, syncer *api.Syncer,
	db *metadb.DB, closer func()) nodefs.File {
	return &WritableFile{
		fd: f,
		File: NewUnimplementedFile(),
		Id: id,
		Name: name,
		syncer: syncer,
		closer: closer,
		db: db,
	}
}

// WritableFile is a fuse file that passes all writes to a temporary file on the
// local disk which is then uploaded once the file is closed.
type WritableFile struct {
	fd *os.File

	// Id is the Google Drive identifier for this file.
	Id   string
	Name string

	// syncer uploads file contents asynchronously.
	syncer *api.Syncer

	// os.File is not threadsafe. Although fd themselves are
	// constant during the lifetime of an open file, the OS may
	// reuse the fd number after it is closed. When open races
	// with another close, they may lead to confusion as which
	// file gets written in the end.
	lock sync.Mutex

	// We embed a nodefs.NewDefaultFile() that returns ENOSYS for every
	// operation we have not implemented. This prevents build breakage when the
	// go-fuse library adds new methods to the nodefs.File interface.
	nodefs.File

	db *metadb.DB

	// closer is a callback that is called when this file has been closed for
	// writing.
	closer func()
}

func (f *WritableFile) InnerFile() nodefs.File {
	return nil
}

func (f *WritableFile) SetInode(n *nodefs.Inode) {
}

func (f *WritableFile) String() string {
	return fmt.Sprintf("WritableFile(%s)", f.Name)
}

func (f *WritableFile) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	f.lock.Lock()
	// This is not racy by virtue of the kernel properly
	// synchronizing the open/write/close.
	r := fuse.ReadResultFd(f.fd.Fd(), off, len(buf))
	f.lock.Unlock()
	return r, fuse.OK
}

func (f *WritableFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	f.lock.Lock()
	n, err := f.fd.WriteAt(data, off)
	f.lock.Unlock()
	return uint32(n), fuse.ToStatus(err)
}

func (f *WritableFile) Release() {
	log.Printf("stat")
	info, err := f.fd.Stat()
	log.Printf("stat: %v", info)

	name := f.fd.Name()

	if err != nil {
		log.Printf("error stating file %s: %v", name, err)
	}

	f.lock.Lock()
	f.fd.Close()
	f.lock.Unlock()

	// Now attempt to upload the file.
	log.Printf("EnqueueFile")
	err = f.syncer.EnqueueFile(f.Id, name)
	if err != nil {
		log.Printf("error enqueuing file %s for upload: %v", name, err)
	}

	log.Printf("SetSize %s %d", f.Name, info.Size())
	err = f.db.SetSize(f.Name, uint64(info.Size()))
	if err != nil {
		log.Printf("error setting size for file %s: %v", name, err)
	}

	// Unlock the file for writing.
	f.closer()
}

func (f *WritableFile) Flush() fuse.Status {
	f.lock.Lock()

	// Since Flush() may be called for each dup'd fd, we don't
	// want to really close the file, we just want to flush. This
	// is achieved by closing a dup'd fd.
	newFd, err := syscall.Dup(int(f.fd.Fd()))
	f.lock.Unlock()

	if err != nil {
		return fuse.ToStatus(err)
	}
	err = syscall.Close(newFd)
	return fuse.ToStatus(err)
}

func (f *WritableFile) Fsync(flags int) (code fuse.Status) {
	f.lock.Lock()
	r := fuse.ToStatus(syscall.Fsync(int(f.fd.Fd())))
	f.lock.Unlock()

	return r
}

func (f *WritableFile) Truncate(size uint64) fuse.Status {
	f.lock.Lock()
	r := fuse.ToStatus(syscall.Ftruncate(int(f.fd.Fd()), int64(size)))
	f.lock.Unlock()

	return r
}

func (f *WritableFile) Chmod(mode uint32) fuse.Status {
	// FIXME(simon): These need to go through the metadata cache, not here.
	f.lock.Lock()
	r := fuse.ToStatus(f.fd.Chmod(os.FileMode(mode)))
	f.lock.Unlock()

	return r
}

func (f *WritableFile) Chown(uid uint32, gid uint32) fuse.Status {
	// FIXME(simon): These need to go through the metadata cache, not here.
	f.lock.Lock()
	r := fuse.ToStatus(f.fd.Chown(int(uid), int(gid)))
	f.lock.Unlock()

	return r
}

func (f *WritableFile) GetAttr(a *fuse.Attr) fuse.Status {
	// FIXME(simon): These need to go through the metadata cache, not here.
	st := syscall.Stat_t{}
	f.lock.Lock()
	err := syscall.Fstat(int(f.fd.Fd()), &st)
	f.lock.Unlock()
	if err != nil {
		return fuse.ToStatus(err)
	}
	a.FromStat(&st)

	return fuse.OK
}
