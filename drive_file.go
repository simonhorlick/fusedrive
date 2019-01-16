package main

import (
	"bytes"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/simonhorlick/fusedrive/api"
	"github.com/simonhorlick/fusedrive/serialize_reads"
	"google.golang.org/api/drive/v3"
	"io"
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
		// Create a write buffer that has capacity for a single write.
		dataBuffer: make([]byte, 0, fuse.MAX_KERNEL_WRITE),
	}
}

type DriveFile struct {
	driveApi *api.DriveApi

	api.DriveApiFile

	// reader is a read buffer for this file. Data is requested from the api in
	// large chunks to increase throughput and buffered here until it is
	// requested. This helps with sequential reads where fuse requests many
	// small chunks of data.
	reader io.Reader

	// readerPosition is the current position of reader in the file. If a read
	// comes in for an offset that isn't equal to readerPosition we dispose of
	// the reader and create a new one.
	readerPosition int64

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

// Read reads a range of bytes from the remote file. What happens here is that
// fuse requests many small reads which if sent directly to the remote will
// incur huge latency and slow down reads drastically. Instead we open a Reader
// that begins streaming data from the given offset in the file using much
// larger block sizes to increase throughput. It's very likely that subsequent
// calls to Read will be sequential, so we can continue taking data from the
// Reader and advancing it each time.
func (f *DriveFile) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	log.Printf("DriveFile Read request for %s at offset %d bufsize %d", f.Name, off, len(buf))

	// Read requests can arrive out of order, which kills sequential read
	// performance. Attempt to re-order them by waiting to see what other
	// requests come in.
	serialize_reads.Wait(off, len(buf))
	defer serialize_reads.Done()

	log.Printf("DriveFile reading %s at offset %d bufsize %d", f.Name, off, len(buf))

	// If we can reuse an existing reader, then do that. Otherwise create a new
	// reader at the given offset.
	if f.reader == nil {
		log.Printf("DriveFile New reader created at offset %d", off)
		// Start off by assuming sequential reads, if the reads aren't
		// sequential then we'll re-create the reader and mark it
		// non-sequential.
		f.reader = api.NewFileReader(f.driveApi, f.Id,
			f.Size, off, true)
	} else if f.readerPosition != off {
		log.Printf("DriveFile Non-sequential read at offset %d, previous read ended at %d",
			off, f.readerPosition)
		//_ = f.reader.Close()
		f.reader = api.NewFileReader(f.driveApi, f.Id, f.Size, off, false)
	}

	n, err := io.ReadFull(f.reader, buf)
	f.readerPosition += int64(n)

	if err == io.EOF {
		log.Printf("DriveFile received EOF")
		return fuse.ReadResultData(buf[:n]), fuse.OK
	}

	if err != nil {
		// TODO(simon): Figure out the correct error code here.
		log.Printf("error reading file: %v", err)
		return nil, fuse.EIO
	}

	log.Printf("DriveFile Read %d bytes", n)

	return fuse.ReadResultData(buf[:n]), fuse.OK
}

func (f *DriveFile) Release() {
	log.Printf("Release %s", f.Name)
	// TODO(simon): Is there anything to do here?
}

func (f *DriveFile) GetAttr(out *fuse.Attr) fuse.Status {
	file, err := f.driveApi.GetAttr(f.Id)
	if err != nil {
		// TODO(simon): Figure out correct return code here.
		return fuse.EAGAIN
	}

	out.Mode = fuse.S_IFREG | 0644
	out.Size = uint64(file.Size)

	log.Printf("GetAttr %s: size is %d", f.Name, file.Size)

	return fuse.OK
}

func (f* DriveFile) Write(data []byte, off int64) (written uint32,
	code fuse.Status) {
	log.Printf("Write (%s) %d bytes at offset %d", f.Name, len(data), off)

	// We buffer writes in memory until the data is flushed.

	// Check whether the buffer is large enough, if not expand it.
	used := len(f.dataBuffer)
	capacity := cap(f.dataBuffer)
	log.Printf("buffer: used %d/%d", used, capacity)

	if capacity < int(off)+len(data) {
		newCapacity := capacity * 2
		log.Printf("buffer: alloc %d/%d", used, newCapacity)

		// Double the buffer size each time we need to reallocate.
		t := make([]byte, used, newCapacity)
		copy(t, f.dataBuffer)
		f.dataBuffer = t[:used]
	}

	// Copy the chunk to the buffer.
	copied := copy(f.dataBuffer[off:int(off)+len(data)], data)
	f.dataBuffer = f.dataBuffer[0:int(off)+len(data)]

	log.Printf("buffer: copied %d, used %d/%d", copied, len(f.dataBuffer), capacity)

	return uint32(copied), fuse.OK
}

// max returns the larger of the two arguments.
func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (f* DriveFile) Flush() fuse.Status {
	if len(f.dataBuffer) == 0 {
		log.Print("Flush nothing to do, no data buffered.")
		return fuse.OK
	}

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