package metadb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/go-errors/errors"
	bolt "go.etcd.io/bbolt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	dbName           = "drive.db"
	dbFilePermission = 0600
)

var (
	// pathsBucket maps absolute paths to attributes
	pathsBucket = []byte("paths-bucket")

	DoesNotExist = errors.New("does not exist")
)

// Attributes describes a node on the filesystem.
type Attributes struct {
	// Id is the Google Drive id for this node.
	Id string

	// Size is the number of bytes stored by this file. For directories this is
	// zero.
	Size uint64

	// IsRegularFile is true for all files and false for directories.
	IsRegularFile bool

	// Mode is the
	Mode uint32
}

func serialiseAttributes(attributes Attributes) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := writeAttributes(buf, attributes)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// writeAttributes ...
func writeAttributes(w io.Writer, attributes Attributes) error {
	if len(attributes.Id) != 33 {
		panic("Google Drive ID is not 33 bytes")
	}

	if _, err := w.Write([]byte(attributes.Id)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, attributes.Size); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, attributes.IsRegularFile); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, attributes.Mode); err != nil {
		return err
	}

	return nil
}

// readAttributes ...
func readAttributes(r io.Reader) (Attributes, error) {
	var attributes Attributes

	id := make([]byte, 33)
	if _, err := io.ReadFull(r, id); err != nil {
		return attributes, err
	}
	attributes.Id = string(id)
	if err := binary.Read(r, binary.LittleEndian, &attributes.Size); err != nil {
		return attributes, err
	}
	if err := binary.Read(r, binary.LittleEndian, &attributes.IsRegularFile); err != nil {
		return attributes, err
	}
	if err := binary.Read(r, binary.LittleEndian, &attributes.Mode); err != nil {
		return attributes, err
	}

	return attributes, nil
}

// DB stores all metadata for the filesystem. This includes attributes for files
// and directories.
type DB struct {
	*bolt.DB
	dbPath string
}

// fileExists returns true if the file exists, and false otherwise.
func fileExists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}

	return true
}

// createDB initialises a new empty database and creates all the buckets
// that are required.
func createDB(dbPath string) error {
	if !fileExists(dbPath) {
		if err := os.MkdirAll(dbPath, 0700); err != nil {
			return err
		}
	}

	path := filepath.Join(dbPath, dbName)
	db, err := bolt.Open(path, dbFilePermission, nil)
	if err != nil {
		return err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket(pathsBucket); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("unable to create new db")
	}

	return db.Close()
}

// Open attempts to open an existing database file, if one doesn't exist then it
// is created.
func Open(dbPath string) (*DB, error) {
	path := filepath.Join(dbPath, dbName)

	if !fileExists(path) {
		if err := createDB(dbPath); err != nil {
			return nil, err
		}
	}

	db, err := bolt.Open(path, dbFilePermission, nil)
	if err != nil {
		return nil, err
	}

	return &DB{DB: db, dbPath: dbPath}, nil
}

func (d *DB) Close() error {
	return d.DB.Close()
}

func serialisePath(path string) []byte {
	return []byte(path)
}

func (d *DB) GetAttributes(path string) (Attributes, error) {
	log.Printf("GetAttributes %s", path)
	var attributes Attributes
	var err error
	err = d.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(pathsBucket)
		v := b.Get(serialisePath(path))
		if v == nil {
			return DoesNotExist
		}
		attributes, err = readAttributes(bytes.NewReader(v))
		return err
	})
	if err != nil {
		return attributes, err
	}

	return attributes, nil
}

func (d *DB) SetAttributes(path string, attributes Attributes) error {
	log.Printf("SetAttributes %s: %v", path, attributes)
	return d.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(pathsBucket)
		v, err := serialiseAttributes(attributes)
		if err != nil {
			return err
		}
		return b.Put(serialisePath(path), v)
	})
}

func (d *DB) GetAndDeleteAttributes(path string) (Attributes, error) {
	log.Printf("GetAndDeleteAttributes %s", path)
	var attributes Attributes
	var err error
	err = d.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(pathsBucket)
		k := serialisePath(path)
		v := b.Get(k)
		if v == nil {
			return DoesNotExist
		}
		attributes, err = readAttributes(bytes.NewReader(v))
		return b.Delete(k)
	})

	return attributes, err
}

type Entry struct {
	Path       string
	Attributes Attributes
}

func (d *DB) List(path string) ([]Entry, error) {
	log.Printf("List %s", path)
	var entries []Entry
	err := d.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(pathsBucket).Cursor()

		prefix := serialisePath(path)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			fmt.Printf("key=%s, value=%s\n", k, v)

			// Skip the directory we're listing.
			if bytes.Equal(k, prefix) {
				continue
			}

			// Find the path of this entry relative to path.
			relativePath := strings.TrimPrefix(string(k), path)
			relativePath = strings.TrimPrefix(relativePath, "/")

			// If the path contains further separators then it's part of a sub-
			// directory and we can exclude it.
			if strings.Contains(relativePath, "/") {
				continue
			}

			attributes, err := readAttributes(bytes.NewReader(v))
			if err != nil {
				return err
			}
			entries = append(entries, Entry{
				Path:       relativePath,
				Attributes: attributes,
			})
		}

		return nil
	})

	return entries, err
}

func (d *DB) SetSize(path string, size uint64) error {
	log.Printf("SetSize %s: %d", path, size)
	return d.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(pathsBucket)

		k := serialisePath(path)
		v := b.Get(k)
		if v == nil {
			return DoesNotExist
		}

		attributes, err := readAttributes(bytes.NewReader(v))
		if err != nil {
			return err
		}

		attributes.Size = size

		updated, err := serialiseAttributes(attributes)
		if err != nil {
			return err
		}
		return b.Put(k, updated)
	})

}