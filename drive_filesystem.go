package main

import (
	"crypto/rand"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	. "github.com/simonhorlick/fusedrive/api"
	"github.com/simonhorlick/fusedrive/metadb"
	"github.com/simonhorlick/fusedrive/serialize_reads"
	"google.golang.org/api/drive/v3"
	"io"
	"io/ioutil"
	"log"
	"math"
	"strings"
	"syscall"
)

// Verify that interface is implemented.
var _ pathfs.FileSystem = &DriveFileSystem{}

// DriveFileSystem exposes the Google Drive api as a fuse filesystem.
type DriveFileSystem struct {
	pathfs.FileSystem
	driveApi *DriveApi

	// db is a database that stores all of the filesystem metadata.
	db     *metadb.DB

	// syncer provides a way to asynchronously upload files to a backing store.
	syncer *Syncer
}

func NewDriveFileSystem(api *DriveApi, db *metadb.DB, syncer *Syncer) pathfs.FileSystem {
	log.Print("Creating DriveFileSystem")
	serialize_reads.InitSerializer()
	return &DriveFileSystem{
		FileSystem: pathfs.NewDefaultFileSystem(),
		driveApi:   api,
		db:         db,
		syncer:     syncer,
	}
}

func (fs *DriveFileSystem) StatFs(name string) *fuse.StatfsOut {
	return &fuse.StatfsOut{
		Blocks: math.MaxUint64,
		Bfree:  math.MaxUint64,
		Bavail: math.MaxUint64,
		Files:  0,
		Ffree:  math.MaxUint64,
		Bsize:  uint32(16 * 1024 * 1024),
		Frsize: uint32(16 * 1024 * 1024),
	}
}

func (fs *DriveFileSystem) OnMount(nodeFs *pathfs.PathNodeFs) {
	log.Printf("OnMount %v", nodeFs)
}

func (fs *DriveFileSystem) OnUnmount() {
	log.Print("OnUnmount")
}

// toFuseAttributes adapts the attributes in the database into fuse attributes.
func toFuseAttributes(attributes metadb.Attributes, out *fuse.Attr) {
	if attributes.IsRegularFile {
		out.Mode = fuse.S_IFREG | attributes.Mode
		out.Size = attributes.Size
	} else {
		out.Mode = fuse.S_IFDIR | attributes.Mode
	}
}

func (fs *DriveFileSystem) GetAttr(name string, context *fuse.Context) (
	*fuse.Attr, fuse.Status) {
	log.Printf("GetAttr \"%s\"", name)

	// The mount point.
	if name == "" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0755}, fuse.OK
	}

	attributes, err := fs.db.GetAttributes(name)

	if err == metadb.DoesNotExist {
		return nil, fuse.ENOENT
	} else if err != nil {
		log.Printf("failed to read file metadata %s: %v", name, err)
		return nil, fuse.ENODATA
	}

	out := new(fuse.Attr)
	toFuseAttributes(attributes, out)

	return out, fuse.OK
}

// OpenDir returns the contents of a directory
func (fs *DriveFileSystem) OpenDir(name string, context *fuse.Context) (
	stream []fuse.DirEntry, status fuse.Status) {
	log.Printf("OpenDir \"%s\"", name)

	entries, err := fs.db.List(name)
	if err != nil {
		log.Printf("failed to read directory listing for %s: %v", name, err)
		return nil, fuse.EIO
	}

	output := make([]fuse.DirEntry, 0)
	for _, entry := range entries {
		// Is this a regular file or a directory?
		var fileType uint32
		if entry.Attributes.IsRegularFile {
			fileType = fuse.S_IFREG
		} else {
			fileType = fuse.S_IFDIR
		}

		d := fuse.DirEntry{
			Name: entry.Path,
			Mode: fileType | entry.Attributes.Mode,
		}
		output = append(output, d)
	}

	return output, fuse.OK
}

// PrintFlags returns a string containing the names of the flags set in flags.
func PrintFlags(flags uint32) string {
	var out []string
	if flags&syscall.O_RDONLY != 0 {
		out = append(out, "O_RDONLY")
	}
	if flags&syscall.O_WRONLY != 0 {
		out = append(out, "O_WRONLY")
	}
	if flags&syscall.O_RDWR != 0 {
		out = append(out, "O_RDWR")
	}
	if flags&syscall.O_APPEND != 0 {
		out = append(out, "O_APPEND")
	}
	if flags&syscall.O_CREAT != 0 {
		out = append(out, "O_CREAT")
	}
	if flags&syscall.O_EXCL != 0 {
		out = append(out, "O_EXCL")
	}
	if flags&syscall.O_TRUNC != 0 {
		out = append(out, "O_TRUNC")
	}
	if flags&syscall.O_NONBLOCK != 0 {
		out = append(out, "O_NONBLOCK")
	}
	if flags&syscall.O_SYNC != 0 {
		out = append(out, "O_SYNC")
	}
	return strings.Join(out, ",")
}

func (fs *DriveFileSystem) Open(name string, flags uint32,
	context *fuse.Context) (fuseFile nodefs.File, status fuse.Status) {
	log.Printf("Open \"%s\" (%s)", name, PrintFlags(flags))

	attributes, err := fs.db.GetAttributes(name)

	if err == metadb.DoesNotExist {
		return nil, fuse.ENOENT
	} else if err != nil {
		log.Printf("failed to read file metadata %s: %v", name, err)
		return nil, fuse.ENODATA
	}

	// If this file is stored in the db, then handle it appropriately.
	if attributes.HasContent {
		return NewDbFile(fs.db, name), fuse.OK
	}

	// TODO(simon): We need to determine if the local copy is the up-to-date
	//  version and return that if so.

	// If we're opening this file read only then we don't need to download the
	// entire file first.
	accessMode := flags & syscall.O_ACCMODE
	if accessMode == syscall.O_RDONLY {
		driveFile := NewDriveFile(fs.driveApi, fs.db, DriveApiFile{
			Name: name,
			Id:   attributes.Id,
			Size: attributes.Size,
		})

		return &nodefs.WithFlags{
			// Disable kernel page cache. This option prevents the kernel from
			// requesting reads non-sequentially.
			FuseFlags: fuse.FOPEN_DIRECT_IO,
			File:      driveFile,
		}, fuse.OK
	} else {
		// Pull this file from the backing store.
		tmpFile, err := ioutil.TempFile("", attributes.Id)
		if err != nil {
			log.Printf("error opening temporary file: %v", err)
			return nil, fuse.EIO
		}

		reader := NewFileReader(fs.driveApi, attributes.Id, attributes.Size, 0,
			true)
		n, err := io.Copy(tmpFile, reader)
		if uint64(n) != attributes.Size {
			panic(fmt.Sprintf(
				"Wrote %d bytes, but file metadata expected %d bytes.", n,
				attributes.Size))
		}

		return NewWritableFile(tmpFile, attributes.Id, fs.syncer), fuse.ENOSYS
	}
}

func RandomBytes() []byte {
	buf := [33]byte{}
	_, err := rand.Read(buf[:])
	if err != nil {
		panic("Unable to generate random int")
	}

	return buf[:]
}

// GenerateId returns a random id in roughly the same format as Google Drive.
func GenerateId() string {
	return string(RandomBytes())
}

func (fs *DriveFileSystem) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	log.Printf("Mkdir \"%s\"", name)

	err := fs.db.SetAttributes(name, metadb.Attributes{
		// This is only ever used locally, so just generate a random id.
		Id:            GenerateId(),
		Size:          0,
		Mode:          mode,
		IsRegularFile: false,
	})
	if err != nil {
		log.Printf("failed to create directory %s: %v", name, err)
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *DriveFileSystem) Rename(oldName string, newName string,
	context *fuse.Context) (code fuse.Status) {
	log.Printf("Rename \"%s\" -> \"%s\"", oldName, newName)

	err := fs.db.Rename(oldName, newName)
	if err == metadb.DoesNotExist {
		return fuse.ENOENT
	}
	if err == metadb.AlreadyExists {
		return fuse.EINVAL
	}

	if err != nil {
		log.Printf("failed to rename file %s: %v", oldName)
		return fuse.EIO
	}

	return fuse.OK
}


func (fs *DriveFileSystem) Create(name string, flags uint32, mode uint32,
	context *fuse.Context) (file nodefs.File, code fuse.Status) {
	// Allow certain files to be stored in the database.
	if strings.HasSuffix(name, "gocryptfs.diriv") {
		log.Printf("Creating file in database \"%s\"", name)

		err := fs.db.SetAttributes(name, metadb.Attributes{
			Id:            GenerateId(),
			Size:          0,
			Mode:          mode,
			IsRegularFile: true,
			HasContent:    true,
		})
		if err != nil {
			log.Printf("failed to create database file %s: %v", name, err)
			return nil, fuse.EIO
		}
		return NewDbFile(fs.db, name), fuse.OK
	}

	log.Printf("Creating file on remote \"%s\"", name)

	newFile := &drive.File{
		Name: name,
	}

	f, err := fs.driveApi.Service.Files.Create(newFile).Do()
	if err != nil {
		log.Printf("failed to create file: %v", err)
		return nil, fuse.EIO
	}

	err = fs.db.SetAttributes(name, metadb.Attributes{
		Id:            f.Id,
		Size:          0,
		Mode:          mode,
		IsRegularFile: true,
	})
	if err != nil {
		log.Printf("failed to create file %s: %v", name, err)
		return nil, fuse.EIO
	}

	log.Printf("Created file: %s", spew.Sdump(f))

	return NewDriveFile(fs.driveApi, fs.db, DriveApiFile{
		Name: f.Name,
		Id:   f.Id,
	}), fuse.OK
}

func (fs *DriveFileSystem) Unlink(name string, context *fuse.Context) (
	code fuse.Status) {
	log.Printf("Unlink \"%s\"", name)

	attributes, err := fs.db.GetAndDeleteAttributes(name)
	if err == metadb.DoesNotExist {
		return fuse.ENOENT
	}

	if err != nil {
		log.Printf("failed to delete metadata for file %s: %v", name, err)
		return fuse.EIO
	}

	err = fs.driveApi.Service.Files.Delete(attributes.Id).Do()
	if err != nil {
		log.Printf("failed to delete file %s on remote: %v", name, err)
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *DriveFileSystem) Rmdir(name string, context *fuse.Context) fuse.Status {
	log.Printf("Rmdir \"%s\"", name)

	empty, err := fs.db.IsDirectoryEmpty(name)
	if err == metadb.DoesNotExist {
		return fuse.ENOENT
	}

	if !empty {
		return fuse.Status(syscall.ENOTEMPTY)
	}

	attributes, err := fs.db.GetAndDeleteAttributes(name)

	if attributes.IsRegularFile {
		return fuse.ENOTDIR
	}

	if err != nil {
		log.Printf("failed to delete metadata for directory %s: %v", name, err)
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *DriveFileSystem) Chmod(name string, mode uint32,
	context *fuse.Context) (code fuse.Status) {
	err := fs.db.SetMode(name, mode)
	if err == metadb.DoesNotExist {
		return fuse.ENOENT
	} else if err != nil {
		return fuse.EIO
	}

	return fuse.OK
}
