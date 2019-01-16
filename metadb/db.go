package metadb

import (
	"fmt"
	bolt "go.etcd.io/bbolt"
	"os"
	"path/filepath"
)

const (
	dbName           = "drive.db"
	dbFilePermission = 0600
)

var (
	// pathsBucket maps absolute paths to attributes
	pathsBucket = []byte("paths-bucket")
)

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