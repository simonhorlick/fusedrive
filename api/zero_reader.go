package api

import (
	"io"
)

var _ io.ReadCloser = &ZeroReader{} // Verify that interface is implemented.

// ZeroReader is an io.Reader that returns zeros.
type ZeroReader struct {
	driveApi *DriveApi
	id string

	// The position of this reader within the file.
	position int64

	// The length of the file.
	length int64
}

func NewZeroReader(driveApi *DriveApi, id string, length, position int64) *ZeroReader {
	return &ZeroReader{
		driveApi: driveApi,
		id: id,
		position: position,
		length: length,
	}
}

// Read implements the io.Reader interface.
func (f *ZeroReader) Read(p []byte) (int, error) {
	remaining := f.length - f.position

	if remaining <= 0 {
		return 0, io.EOF
	}

	n := int(min(uint64(len(p)), uint64(remaining)))
	for i := 0; i < n; i++ {
		p[i] = 0
	}

	f.position += int64(n)

	return n, nil
}

func (f *ZeroReader) Close() error {
	return nil
}