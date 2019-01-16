package main

import (
	"errors"
	"github.com/davecgh/go-spew/spew"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	. "github.com/simonhorlick/fusedrive/api"
	"github.com/simonhorlick/fusedrive/serialize_reads"
	"google.golang.org/api/drive/v3"
	"log"
	"math"
	"strings"
	"sync"
)

// Verify that interface is implemented.
var _ pathfs.FileSystem = &DriveFileSystem{}

// DriveFileSystem exposes the Google Drive api as a fuse filesystem.
type DriveFileSystem struct {
	pathfs.FileSystem
	driveApi *DriveApi

	// pathIdMap maps absolute file paths to their Google Drive ID.
	pathIdMap map[string]string

	// idMapMutex synchronises access to the pathIdMap.
	idMapMutex sync.RWMutex
}

func NewDriveFileSystem(api *DriveApi) pathfs.FileSystem {
	log.Print("Creating DriveFileSystem")
	serialize_reads.InitSerializer()
	return &DriveFileSystem{
		FileSystem: pathfs.NewDefaultFileSystem(),
		driveApi:   api,
		pathIdMap:  make(map[string]string),
	}
}

func (fs *DriveFileSystem) StatFs(name string) *fuse.StatfsOut {
	log.Printf("StatFs %s", name)
	return &fuse.StatfsOut{
		Blocks: math.MaxUint64,
		Bfree: math.MaxUint64,
		Bavail: math.MaxUint64,
		Files: 0,
		Ffree: math.MaxUint64,
		Bsize: uint32(16 * 1024 * 1024),
		Frsize: uint32(16 * 1024 * 1024),
	}
}

func (fs *DriveFileSystem) OnMount(nodeFs *pathfs.PathNodeFs) {
	log.Printf("OnMount %v", nodeFs)
}

func (fs *DriveFileSystem) OnUnmount() {
	log.Print("OnUnmount")
}

func (fs *DriveFileSystem) LookupByPath(path string) (string, error) {
	fs.idMapMutex.RLock()
	id, ok := fs.pathIdMap[path]
	fs.idMapMutex.RUnlock()

	if ok {
		// Cache hit.
		return id, nil
	} else {
		// Cache miss, try and look up on the api.
		f := fs.driveApi.GetByName(path)
		if f == nil {
			return "", errors.New("not found")
		}

		// Insert the id into the map.
		fs.idMapMutex.Lock()
		fs.pathIdMap[path] = f.Id
		fs.idMapMutex.Unlock()

		return f.Id, nil
	}
}

func (fs *DriveFileSystem) GetAttr(name string, context *fuse.Context) (
	a *fuse.Attr, code fuse.Status) {
	log.Printf("GetAttr \"%s\"", name)

	// The mount point.
	if name == "" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0755}, fuse.OK
	}

	id, err := fs.LookupByPath(name)
	if err != nil {
		log.Printf("error: %v", err)
		return nil, fuse.ENOENT
	}

	file, err := fs.driveApi.GetAttr(id)
	if err != nil {
		log.Printf("error getting file attributes: %v", err)
		return nil, fuse.EIO
	}

	var out fuse.Attr
	if file.MimeType == directoryMimeType {
		out.Mode = fuse.S_IFDIR | 0755
	} else {
		out.Mode = fuse.S_IFREG | 0644
		out.Size = uint64(file.Size)
	}

	return &out, fuse.OK
}

// OpenDir returns the contents of a directory
func (fs *DriveFileSystem) OpenDir(name string, context *fuse.Context) (
	stream []fuse.DirEntry, status fuse.Status) {
	log.Printf("OpenDir \"%s\"", name)

	output := make([]fuse.DirEntry, 0)

	for _, entry := range fs.driveApi.List() {
		// Files from sub-directories are returned with slashes in their names,
		// exclude these from the listing.
		if strings.HasPrefix(entry.Name, name) {
			relative := strings.TrimPrefix(entry.Name, name)
			relative = strings.TrimPrefix(relative, "/")

			// If the path contains further separators then it's part of a sub-
			// directory and we can exclude it.
			if strings.Contains(relative, "/") {
				log.Printf("  %s (%s) SUBDIRECTORY", relative, entry.Id)
			} else {
				log.Printf("  %s (%s)", relative, entry.Id)
				d := fuse.DirEntry{
					Name: relative,
					Mode: fuse.S_IFREG | 0644,
				}
				output = append(output, d)
			}
		}
	}

	return output, fuse.OK
}

func (fs *DriveFileSystem) Open(name string, flags uint32,
	context *fuse.Context) (fuseFile nodefs.File, status fuse.Status) {
	log.Printf("Open \"%s\"", name)

	id, err := fs.LookupByPath(name)
	if err != nil {
		log.Printf("error: %v", err)
		return nil, fuse.ENOENT
	}

	// TODO(simon): This is wasteful, but we need to know the size.
	file, err := fs.driveApi.GetAttr(id)
	if err != nil {
		log.Printf("error getting file attributes: %v", err)
		return nil, fuse.EIO
	}

	driveFile := NewDriveFile(fs.driveApi, DriveApiFile{
		Name: name,
		Id: id,
		Size: file.Size,
	})

	return &nodefs.WithFlags{
		// Disable kernel page cache. This option prevents the kernel from
		// requesting reads non-sequentially.
		FuseFlags: fuse.FOPEN_DIRECT_IO,
		File: driveFile,
	}, fuse.OK
}

const directoryMimeType = "application/vnd.google-apps.folder"

func (fs *DriveFileSystem) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	// A directory is a File with a specific mime type.
	newFile := &drive.File{
		Name: name,
		MimeType: directoryMimeType,
	}

	f, err := fs.driveApi.Service.Files.Create(newFile).Do()
	if err != nil {
		log.Printf("failed to create directory: %v", err)
		return fuse.EIO
	}

	log.Printf("created directory: %s", spew.Sdump(f))

	return fuse.OK
}

func (fs *DriveFileSystem) Create(name string, flags uint32, mode uint32,
	context *fuse.Context) (file nodefs.File, code fuse.Status) {
	log.Printf("Creating file \"%s\"", name)

	newFile := &drive.File{
		Name: name,
	}

	f, err := fs.driveApi.Service.Files.Create(newFile).Do()
	if err != nil {
		log.Printf("failed to create file: %v", err)
		return nil, fuse.EIO
	}

	log.Printf("created file: %s", spew.Sdump(f))

	return NewDriveFile(fs.driveApi, DriveApiFile{
		Name: f.Name,
		Id: f.Id,
	}), fuse.OK
}

func (fs *DriveFileSystem) Unlink(name string, context *fuse.Context) (
	code fuse.Status) {
	id, err := fs.LookupByPath(name)
	if err != nil {
		log.Printf("error: %v", err)
		return fuse.ENOENT
	}

	// Remove the entry from the id cache.
	fs.idMapMutex.Lock()
	delete(fs.pathIdMap, name)
	fs.idMapMutex.Unlock()

	err = fs.driveApi.Service.Files.Delete(id).Do()
	if err != nil {
		log.Printf("failed to delete file: %v", err)
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *DriveFileSystem) Rmdir(name string, context *fuse.Context) (
	code fuse.Status) {
	id, err := fs.LookupByPath(name)
	if err != nil {
		log.Printf("error: %v", err)
		return fuse.ENOENT
	}

	// Remove the entry from the id cache.
	fs.idMapMutex.Lock()
	delete(fs.pathIdMap, name)
	fs.idMapMutex.Unlock()

	err = fs.driveApi.Service.Files.Delete(id).Do()
	if err != nil {
		log.Printf("failed to delete directory: %v", err)
		return fuse.EIO
	}

	return fuse.OK
}
