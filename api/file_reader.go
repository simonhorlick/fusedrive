package api

import "log"

// FileReader is an io.Reader that reads a file from Google Drive sequentially.
type FileReader struct {
	driveApi *DriveApi
	id string

	// The position of this reader within the file.
	position int64
}

func NewFileReader(driveApi *DriveApi, id string, position int64) *FileReader {
	return &FileReader{
		driveApi: driveApi,
		id: id,
		position: position,
	}
}

// Read implements the io.Reader interface.
func (f *FileReader) Read(p []byte) (n int, err error) {
	log.Printf("FileReader Read of %d bytes at offset %d", len(p), f.position)

	// Read as many bytes as possible into p.
	n, err = f.driveApi.ReadAt(f.id, p, f.position)

	// Increment the readers position in the file.
	f.position += int64(n)

	return n, err
}
