package main

import (
	"fmt"
	"github.com/simonhorlick/fusedrive/api"
	"github.com/simonhorlick/fusedrive/metadb"
	"github.com/simonhorlick/fusedrive/multimutex"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

func (f *FileReference) Release() {
	f.cache.Release(f)
}

type refcountedFile struct {
	file    *os.File
	id      string
	count   int
	dirty   bool
	fetched bool
}

// LocalFileCache copies files locally and re-uploads them when all clients have
// closed the file.
type LocalFileCache struct {
	api *api.DriveApi

	db *metadb.DB

	// files lists all currently open files, their reference counts and whether
	// they've been written to.
	files   map[string]*refcountedFile

	// filesMu synchronizes access to the files map.
	filesMu sync.Mutex

	// locks provides fine-grained locking over individual path names.
	locks *multimutex.KeyedMutex
}

func NewLocalFileCache(api *api.DriveApi, db *metadb.DB) *LocalFileCache {
	return &LocalFileCache{
		api:   api,
		db:    db,
		files: make(map[string]*refcountedFile),
		locks: multimutex.NewKeyedMutex(),
	}
}

// MarkDirty ensures the given file is marked as changed and put back to the
// remote when all clients have released it.
func (c *LocalFileCache) MarkDirty(file *FileReference) {
	c.filesMu.Lock()
	defer c.filesMu.Unlock()

	info, ok := c.files[file.name]
	if !ok {
		panic(fmt.Sprintf("expected entry for %s in file table", file.name))
	}

	if !info.dirty {
		log.Printf("Marking file %s as dirty", file.name)
	}

	info.dirty = true
}

// Open returns the local file that backs this fuse file. If the file does not
// exist locally then it is created first.
func (c *LocalFileCache) Open(name, id string, isReader bool) *FileReference {
	log.Printf("Open for file %s, read is %s", name, isReader)

	// Take out a lock on this name.
	c.locks.Lock(name)
	defer c.locks.Unlock(name)

	// First check if the file already exists, if we're the first reader then
	// grab the file from gdrive and update the map again. This is safe because
	// we're holding the file-level lock throughout.
	c.filesMu.Lock()
	defer c.filesMu.Unlock()

	info, ok := c.files[name]

	// If this is the first reference to the file then grab the file from gdrive
	// and write it locally.
	if !ok {
		log.Printf("No existing clients for %s", name)

		f, err := ioutil.TempFile("", "")
		if err != nil {
			return nil
		}

		// Fetch the file lazily. Some application will Open a file and never
		// issue reads or writes.
		info = &refcountedFile{
			file: f,
			count: 1,
			dirty: false,
			id: id,
			fetched: false,
		}
		c.files[name] = info
	} else {
		log.Printf("File %s is currently open %d times", name, info.count)
		info.count++
	}

	return &FileReference{
		db: c.db,
		cache: c,
		name: name,
		file: info.file,
		isReader: isReader,
	}
}

func (c *LocalFileCache) IsOpen(name string) bool {
	c.filesMu.Lock()
	defer c.filesMu.Unlock()

	_, isOpen := c.files[name]

	return isOpen
}

func (c *LocalFileCache) Release(file *FileReference) {
	log.Printf("Release %s", file.name)

	// If this was the last reference then delete the file from the
	// local filesystem and upload it back to gdrive.
	c.locks.Lock(file.name)
	defer c.locks.Unlock(file.name)

	// If there are no more references, and if this file is dirty, then
	// re-upload it now. We hold the file lock for the duration to prevent any
	// other clients opening this file while it's being uploaded.
	c.filesMu.Lock()
	refs, ok := c.files[file.name]
	if !ok {
		panic("Expected file to have a reference count!")
	}

	refs.count--

	if refs.count == 0 {
		log.Printf("Reference count for %s is zero, will remove local file",
			file.name)
		delete(c.files, file.name)
	} else {
		log.Printf("Reference count for %s is %d", file.name, refs.count)
	}
	c.filesMu.Unlock()

	if refs.dirty {
		log.Printf("Local file %s is dirty, uploading changes", file.name)

		_, err := refs.file.Seek(0, 0)
		if err != nil {
			log.Printf("failed to seek local file: %v", err)
		}

		if refs.id == EmptyId {
			log.Printf("Creating new file on remote for %s", file.name)
			id, err := c.api.Create(refs.file)
			if err != nil {
				log.Printf("error creating file %s: %v", file.name, err)
			}
			err = c.db.SetId(file.name, id)
			if err != nil {
				log.Printf("failed to set id for file %s: %v", file.name, err)
			}
		} else {
			log.Printf("Updating existing file on remote for %s", file.name)

			err := c.api.Update(refs.id, refs.file)
			if err != nil {
				log.Printf("error updating file %s: %v", file.name, err)
			}
		}

		// Update size.
		info, err := refs.file.Stat()
		err = c.db.SetSize(file.name, uint64(info.Size()))
		if err != nil {
			log.Printf("error setting size for file %s: %v", file.name, err)
		}
	}

	if refs.count == 0 {
		log.Printf("Deleting local file %s", file.name)

		// Close and remove local file
		localPath := refs.file.Name()
		err := refs.file.Close()
		if err != nil {
			log.Printf("failed to close local file: %v", err)
		}
		err = os.Remove(localPath)
		if err != nil {
			log.Printf("failed to remove local file: %v", err)
		}
	}
}

func (c *LocalFileCache) EnsureLocal(file *FileReference) error {
	c.locks.Lock(file.name)
	defer c.locks.Unlock(file.name)

	c.filesMu.Lock()
	defer c.filesMu.Unlock()

	refs, ok := c.files[file.name]
	if !ok {
		panic(fmt.Sprintf("expected files entry for %s", file.name))
	}

	if !refs.fetched {
		if refs.id != EmptyId {
			log.Printf("Reading entire file %s (%s) from remote", file.name, refs.id)
			err := c.api.ReadAll(refs.id, file.file)
			if err != nil {
				log.Printf("Error reading file: %v", err)
				return err
			}
		}
		refs.fetched = true
	}

	return nil
}
