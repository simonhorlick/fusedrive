package api

import (
	"fmt"
	"io"
	"log"
)

// Some observations: throughput continually increases as this value increases
// and setting it crazy high is beneficial at the expense of possibly wasted
// downloads. Fetching chunks in parallel doesn't seem to improve throughput
// and instead hurts time-to-first-byte.
const defaultSequentialReadSize = 512 * 1024 * 1024
const defaultRandomReadSize = 4 * 1024 * 1024

// min returns the smaller of a and b.
func min(a int64, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

var _ io.ReadCloser = &FileReader{} // Verify that interface is implemented.

// ZeroReader is an io.Reader that reads a file from Google Drive sequentially.
// It is designed to perform well even when faced with a large number of small
// read requests.
//
// This is NOT thread safe.
type FileReader struct {
	driveApi *DriveApi
	id string

	// The position of this reader within the file.
	position int64

	// The length of the file.
	length int64

	// The current active http response.
	httpResponse io.ReadCloser

	// The amount of data to read from the api.
	readSize int64
}

func NewFileReader(driveApi *DriveApi, id string, length, position int64,
	sequential bool) *FileReader {

	// If we're reading sequentially then fetch as much data as possible in each
	// api call. If we're reading randomly, then just fetch the minimum.
	var readSize int64
	if sequential {
		readSize = defaultSequentialReadSize
	} else {
		readSize = defaultRandomReadSize
	}

	return &FileReader{
		driveApi: driveApi,
		id: id,
		position: position,
		length: length,
		readSize: readSize,
	}
}

// ReadAt begins streaming the given range of bytes from this file.
func (f *FileReader) ReadAt(size int64, off int64) (io.ReadCloser, error) {
	log.Printf("Sending HTTP request for %d bytes at offset %d ", size, off)

	// The byte range specified in the Range header is [start,end] inclusive. So
	// [0,1023] would return 1024 bytes.
	startRange := off
	endRange := startRange + size - 1

	request := f.driveApi.Service.Files.Get(f.id)
	request.Header().Add("Range",
		fmt.Sprintf("bytes=%d-%d", startRange, endRange))

	response, err := request.Download()
	if err != nil {
		log.Printf("Response error %v", err)
		return nil, err
	}

	return response.Body, nil
}

// Read implements the io.Reader interface.
func (f *FileReader) Read(p []byte) (int, error) {
	//log.Printf("FileReader Read of %d bytes at offset %d", len(p), f.position)

	totalRead := 0

	for len(p) > 0 {
		remainingBytes := f.length - f.position

		// If we've read the whole thing then return end-of-file.
		if remainingBytes == 0 {
			return totalRead, io.EOF
		}

		// Start a new http request if there isn't already one in progress.
		if f.httpResponse == nil {
			requestSize := min(remainingBytes, f.readSize)
			log.Printf("Sending http request for %d bytes, remaining %d bytes",
				requestSize, remainingBytes)

			// Start the request.
			resp, err := f.ReadAt(requestSize, f.position)
			if err != nil {
				log.Printf("Error calling ReadAt: %v", err)
				// TODO(simon): Handle retries properly here.
				// handle http 416 range not satisfiable
				return totalRead, err
			}
			f.httpResponse = resp

			// TODO(simon): If we've placed bytes in p already and have just
			// started a new http request then we can immediately return the
			// bytes in p while the http response comes in.
		}

		// Try and fill p.
		n, err := io.ReadFull(f.httpResponse, p)

		// Increment the readers position in the file.
		f.position += int64(n)
		totalRead += n

		// Point p at the next available space in the buffer.
		p = p[n:]

		//log.Printf("http request returned %d bytes: %v", n, err)

		// Handle end of file for one chunk.
		if err == io.EOF {
			log.Printf("EOF for http request")
			closeErr := f.httpResponse.Close()
			f.httpResponse = nil

			if closeErr != nil {
				log.Printf("error: failed to close http response body: %v",
					closeErr)
			}

			// If possible start a new http request and continue filling p.
			continue
		} else if err != nil {
			return totalRead, err
		}
	}

	return totalRead, nil
}

func (f *FileReader) Close() error {
	// If there's an open http response then close it.
	if f.httpResponse != nil {
		err := f.httpResponse.Close()
		f.httpResponse = nil
		return err
	}

	return nil
}