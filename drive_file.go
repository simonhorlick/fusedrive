package main

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/simonhorlick/fusedrive/api"
	"log"
	"sync"
)

var _ nodefs.File = &DriveFile{} // Verify that interface is implemented.

func NewDriveFile(driveApi *api.DriveApi, file api.DriveApiFile) nodefs.File {
	return &DriveFile{
		driveApi: driveApi,
		File:     nodefs.NewDefaultFile(),
		DriveApiFile: file,
	}
}

type DriveFile struct {
	driveApi *api.DriveApi

	api.DriveApiFile

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
}

func (f *DriveFile) InnerFile() nodefs.File {
	return nil
}

func (f *DriveFile) String() string {
	return fmt.Sprintf("DriveFile(%s)", f.Name)
}

func (f *DriveFile) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	log.Printf("Read %s at offset %d", f.Name, off)
	log.Printf("Read buffer size is %d", len(buf))

	f.lock.Lock()
	// TODO(simon): Check the number of bytes read matches?
	n, err := f.driveApi.ReadAt(f.Id, buf, off)
	f.lock.Unlock()

	if err != nil {
		// TODO(simon): Figure out the correct error code here.
		log.Printf("error reading file: %v", err)

		return nil, fuse.EIO
	}

	log.Printf("Read %d bytes: %x", n, buf)

	return fuse.ReadResultData(buf), fuse.OK
}

func (f *DriveFile) Release() {
	log.Printf("Release %s", f.Name)
	// TODO(simon): Is there anything to do here?
}

func (f *DriveFile) GetAttr(out *fuse.Attr) fuse.Status {
	log.Printf("GetAttr %s", f.Name)

	file, err := f.driveApi.GetAttr(f.Id)
	if err != nil {
		// TODO(simon): Figure out correct return code here.
		return fuse.EAGAIN
	}

	out.Mode = fuse.S_IFREG | 0644
	out.Size = uint64(file.Size)
	return fuse.OK
}
