package main

import (
	"bytes"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/simonhorlick/fusedrive/api"
	"google.golang.org/api/drive/v3"
	"log"
	"sync"
)

var _ nodefs.File = &DriveFile{} // Verify that interface is implemented.

const binaryMimeType = "application/octet-stream"

func NewDriveFile(driveApi *api.DriveApi, file api.DriveApiFile) nodefs.File {
	return &DriveFile{
		driveApi: driveApi,
		File:     NewUnimplementedFile(),
		DriveApiFile: file,
		dataBuffer: make([]byte, 0),
	}
}

type DriveFile struct {
	driveApi *api.DriveApi

	api.DriveApiFile

	// dataBuffer buffers writes until Flush is called.
	dataBuffer []byte

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
	log.Printf("Read %s at offset %d bufsize %d", f.Name, off, len(buf))

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

func (f* DriveFile) Write(data []byte, off int64) (written uint32,
	code fuse.Status) {
	log.Printf("Write %s offset %d", f.Name, off)

	// We buffer writes in memory until the data is flushed.

	// Check whether the buffer is large enough, if not expand it.
	if cap(f.dataBuffer) < int(off)+len(data) {
		t := make([]byte, int(off)+len(data))
		copy(t, f.dataBuffer)
		f.dataBuffer = t
	}

	// Copy the chunk to the buffer.
	copy(f.dataBuffer[off:], data)

	log.Printf("buffer is currently %d", len(f.dataBuffer))

	return uint32(len(data)), fuse.OK
}

func (f* DriveFile) Flush() fuse.Status {
	// TODO(simon): Does this upload the whole file?
	request := f.driveApi.Service.Files.Update(f.Id, &drive.File{
		MimeType: binaryMimeType,
	})
	request.Media(bytes.NewReader(f.dataBuffer))

	log.Printf("Uploading file %d bytes", len(f.dataBuffer))
	file, err := request.Do()
	if err != nil {
		log.Printf("Error uploading file, err: %#v, %v", file, err)
		return fuse.EIO
	}

	log.Printf("Updated file, err: %#v, %v", file, err)
	return fuse.OK
}

// The truncate() and ftruncate() functions cause the regular file named
// by path or referenced by fd to be truncated to a size of precisely
// length bytes.
//
// If the file previously was larger than this size, the extra data is
// lost.  If the file previously was shorter, it is extended, and the
// extended part reads as null bytes ('\0').
func (f *DriveFile) Truncate(size uint64) fuse.Status {
	// TODO(simon): Do we need to implement this?
	if size != 0 {
		log.Printf("error: truncating file to non-zero size is not implemented")
		return fuse.ENOSYS
	}

	request := f.driveApi.Service.Files.Update(f.Id, &drive.File{})
	request.Media(bytes.NewReader([]byte{}))
	file, err := request.Do()
	log.Printf("Updated file, err: %#v, %v", file, err)

	if err != nil {
		log.Printf("error truncating file: %v", err)
		return fuse.EIO
	}

	return fuse.OK
}