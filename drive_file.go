package main

import (
	"bytes"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/simonhorlick/fusedrive/api"
	"github.com/simonhorlick/fusedrive/metadb"
	"github.com/simonhorlick/fusedrive/serialize_reads"
	"google.golang.org/api/drive/v3"
	"io"
	"log"
	"sync"
)

var _ nodefs.File = &DriveFile{} // Verify that interface is implemented.

func NewDriveFile(driveApi *api.DriveApi, db *metadb.DB, file api.DriveApiFile) nodefs.File {
	return &DriveFile{
		driveApi:     driveApi,
		File:         NewUnimplementedFile(),
		DriveApiFile: file,
		db:           db,
		lastReadData: make([]byte, 0, fuse.MAX_KERNEL_WRITE),
	}
}

type DriveFile struct {
	driveApi *api.DriveApi

	api.DriveApiFile

	// The database to store file metadata.
	db *metadb.DB

	// reader is a read buffer for this file. Data is requested from the api in
	// large chunks to increase throughput and buffered here until it is
	// requested. This helps with sequential reads where fuse requests many
	// small chunks of data.
	reader io.ReadCloser

	// We cache the most recent read in memory in case we get more reads for
	// this chunk in non-sequential order.
	lastReadOffset int64
	lastReadData   []byte

	// readerPosition is the current position of reader in the file. If a read
	// comes in for an offset that isn't equal to readerPosition we dispose of
	// the reader and create a new one.
	readerPosition int64

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
	log.Printf("Read for %s at offset %d bufsize %d", f.Name, off, len(buf))

	// Read requests can arrive out of order, which kills sequential read
	// performance. Attempt to re-order them by waiting to see what other
	// requests come in.
	serialize_reads.Wait(off, len(buf))
	defer serialize_reads.Done()

	//log.Printf("DriveFile reading %s at offset %d bufsize %d", f.Name, off, len(buf))

	// If we can reuse an existing reader, then do that. Otherwise create a new
	// reader at the given offset.
	if f.reader == nil {
		log.Printf("DriveFile New reader created at offset %d", off)
		// Start off by assuming sequential reads, if the reads aren't
		// sequential then we'll re-create the reader and mark it
		// non-sequential.
		f.reader = api.NewFileReader(f.driveApi, f.Id,
			f.Size, uint64(off), true)
	} else if f.readerPosition != off {
		// If this is a re-read of the previously fetched chunk, then return
		// that.
		if off >= f.lastReadOffset && off < f.lastReadOffset+int64(len(f.lastReadData)) {
			offsetInLastRead := off - f.lastReadOffset

			n := copy(buf, f.lastReadData[offsetInLastRead:])

			log.Printf("Returning %d bytes [%d, %d] from last read", n, off,
				int64(n)+off-1)

			return fuse.ReadResultData(buf[:n]), fuse.OK
		}

		log.Printf("DriveFile Non-sequential read at offset %d, reader is currently at %d",
			off, f.readerPosition)
		_ = f.reader.Close()
		f.reader = api.NewFileReader(f.driveApi, f.Id, f.Size, uint64(off), false)
		f.readerPosition = off
	}

	remaining := f.Size - uint64(f.readerPosition)

	// buf might be larger than the remaining data in the file, in that case
	// read as much data as there is remaining. Otherwise just fill buf.
	n, err := io.ReadAtLeast(f.reader, buf, min(int(remaining), len(buf)))
	f.readerPosition += int64(n)

	log.Printf("Returning %d bytes [%d, %d], next byte is at %d", n, off,
		int64(n)+off-1, f.readerPosition)

	f.lastReadOffset = off
	copy(f.lastReadData[:n], buf[:n])
	f.lastReadData = f.lastReadData[:n]

	if err == io.EOF {
		log.Printf("DriveFile received EOF")
		return fuse.ReadResultData(buf[:n]), fuse.OK
	}

	if err != nil {
		// TODO(simon): Figure out the correct error code here.
		log.Printf("error reading file: %v", err)
		return nil, fuse.EIO
	}

	//log.Printf("DriveFile Read %d bytes", n)

	return fuse.ReadResultData(buf[:n]), fuse.OK
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (f *DriveFile) Release() {
	log.Printf("Release %s", f.Name)
	// TODO(simon): Is there anything to do here?
}

func (f *DriveFile) GetAttr(out *fuse.Attr) fuse.Status {
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

func (f *DriveFile) Write(data []byte, off int64) (written uint32,
	code fuse.Status) {
	// This is a read-only file.
	return 0, fuse.EPERM
}

// max returns the larger of the two arguments.
func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (f *DriveFile) Flush() fuse.Status {
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

	err = f.db.SetSize(f.Name, size)
	if err != nil {
		log.Printf("error storing file metadata %s: %v", f.Name, err)
		return fuse.EIO
	}

	return fuse.OK
}
