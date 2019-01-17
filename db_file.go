package main

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/simonhorlick/fusedrive/metadb"
	"log"
	"sync"
)

var _ nodefs.File = &DbFile{} // Verify that interface is implemented.

func NewDbFile(db *metadb.DB, name string) nodefs.File {
	return &DbFile{
		File:         NewUnimplementedFile(),
		db:           db,
		Name: name,
	}
}

// DbFile is a fuse file that is stored in the database. This is used for small
// files that are accessed frequently and need low latency.
type DbFile struct {
	// The database to store file metadata.
	db *metadb.DB

	// The absolute path of this file.
	Name string

	// We embed a nodefs.NewDefaultFile() that returns ENOSYS for every
	// operation we have not implemented. This prevents build breakage when the
	// go-fuse library adds new methods to the nodefs.File interface.
	nodefs.File

	writeLock sync.Mutex
}

func (f *DbFile) InnerFile() nodefs.File {
	return nil
}

func (f *DbFile) String() string {
	return fmt.Sprintf("DbFile(%s)", f.Name)
}

func (f *DbFile) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	log.Printf("DbFile Read request for %s at offset %d bufsize %d", f.Name, off, len(buf))

	content, err := f.db.GetFile(f.Name)
	if err != nil {
		log.Printf("error reading file: %v", err)
		return nil, fuse.EIO
	}

	return fuse.ReadResultData(content[off:]), fuse.OK
}

func (f *DbFile) Release() {
	log.Printf("Release %s", f.Name)
}

func (f *DbFile) GetAttr(out *fuse.Attr) fuse.Status {
	log.Printf("GetAttr \"%s\"", f.Name)

	attributes, err := f.db.GetAttributes(f.Name)

	if err == metadb.DoesNotExist {
		return fuse.ENOENT
	} else if err != nil {
		log.Printf("failed to read file metadata %s: %v", f.Name, err)
		return fuse.ENODATA
	}

	toFuseAttributes(attributes, out)

	return fuse.OK
}

func (f *DbFile) Write(data []byte, off int64) (written uint32,
	code fuse.Status) {
	log.Printf("Write (%s) %d bytes at offset %d", f.Name, len(data), off)

	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	content, err := f.db.GetFile(f.Name)
	if err != nil {
		log.Printf("error writing file: %v", err)
		return 0, fuse.EIO
	}

	if len(content) < int(off) + len(data) {
		t := make([]byte, int(off) + len(data))
		copy(t, content)
		content = t
	}

	n := copy(content[off:], data)

	log.Printf("Wrote %d bytes, size is now %d", n, len(content))

	err = f.db.PutFile(f.Name, content)
	if err != nil {
		log.Printf("error writing file: %v", err)
		return 0, fuse.EIO
	}

	err = f.db.SetSize(f.Name, uint64(len(content)))
	if err != nil {
		log.Printf("error writing size: %v", err)
		return 0, fuse.EIO
	}

	return uint32(n), fuse.OK
}

func (f *DbFile) Flush() fuse.Status {
	return fuse.OK
}

func (f *DbFile) Truncate(size uint64) fuse.Status {
	log.Printf("Truncate (%s) %d bytes", f.Name, size)

	f.writeLock.Lock()
	defer f.writeLock.Unlock()

	content, err := f.db.GetFile(f.Name)
	if err != nil {
		log.Printf("error writing file: %v", err)
		return fuse.EIO
	}

	if len(content) < int(size) {
		t := make([]byte, size)
		copy(t, content)
		content = t
	}

	content = content[:size]

	err = f.db.PutFile(f.Name, content)
	if err != nil {
		log.Printf("error writing file: %v", err)
		return fuse.EIO
	}

	err = f.db.SetSize(f.Name, uint64(len(content)))
	if err != nil {
		log.Printf("error writing size: %v", err)
		return fuse.EIO
	}

	log.Printf("Wrote (%s) %d bytes", f.Name, len(content))

	return fuse.OK
}
