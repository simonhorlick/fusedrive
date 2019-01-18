package api

import "io"

// Remote represents a remote filesystem.
type Remote interface {
	// Create uploads a new file to the remote and returns the id of the created
	// file.
	Create(reader io.Reader) (string, error)

	// Update replaces the contents of the given file with the data from reader.
	Update(id string, reader io.Reader) error

	// ReadAt returns the content of the file in the given range with the given
	// id.
	ReadAt(id string, size uint64, off uint64) (io.ReadCloser, error)
}