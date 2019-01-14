package main

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"log"
	"time"
)

type unimplementedFile struct{}

// NewUnimplementedFile returns a File instance that returns ENOSYS for
// every operation and logs any calls.
func NewUnimplementedFile() nodefs.File {
	return &unimplementedFile{}
}

func (f *unimplementedFile) SetInode(*nodefs.Inode) {
	log.Print("unimplemented: SetInode")
}

func (f *unimplementedFile) InnerFile() nodefs.File {
	log.Print("unimplemented: InnerFile")
	return nil
}

func (f *unimplementedFile) String() string {
	return "unimplementedFile"
}

func (f *unimplementedFile) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	log.Print("unimplemented: Read")
	return nil, fuse.ENOSYS
}

func (f *unimplementedFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	log.Print("unimplemented: Write")
	return 0, fuse.ENOSYS
}

func (f *unimplementedFile) GetLk(owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) (code fuse.Status) {
	log.Print("unimplemented: GetLk")
	return fuse.ENOSYS
}

func (f *unimplementedFile) SetLk(owner uint64, lk *fuse.FileLock, flags uint32) (code fuse.Status) {
	log.Print("unimplemented: SetLk")
	return fuse.ENOSYS
}

func (f *unimplementedFile) SetLkw(owner uint64, lk *fuse.FileLock, flags uint32) (code fuse.Status) {
	log.Print("unimplemented: SetLkw")
	return fuse.ENOSYS
}

func (f *unimplementedFile) Flush() fuse.Status {
	log.Print("unimplemented: Flush")
	return fuse.OK
}

func (f *unimplementedFile) Release() {
	log.Print("unimplemented: Release")
}

func (f *unimplementedFile) GetAttr(*fuse.Attr) fuse.Status {
	log.Print("unimplemented: GetAttr")
	return fuse.ENOSYS
}

func (f *unimplementedFile) Fsync(flags int) (code fuse.Status) {
	log.Print("unimplemented: Fsync")
	return fuse.ENOSYS
}

func (f *unimplementedFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	log.Print("unimplemented: Utimens")
	return fuse.ENOSYS
}

func (f *unimplementedFile) Truncate(size uint64) fuse.Status {
	log.Print("unimplemented: Truncate")
	return fuse.ENOSYS
}

func (f *unimplementedFile) Chown(uid uint32, gid uint32) fuse.Status {
	log.Print("unimplemented: Chown")
	return fuse.ENOSYS
}

func (f *unimplementedFile) Chmod(perms uint32) fuse.Status {
	log.Print("unimplemented: Chmod")
	return fuse.ENOSYS
}

func (f *unimplementedFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	log.Print("unimplemented: Allocate")
	return fuse.ENOSYS
}
