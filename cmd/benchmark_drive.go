package main

import (
	"bufio"
	"fmt"
	"github.com/simonhorlick/fusedrive/api"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"time"
)

const fileId = "1kgcI9l0qzeB8LtmUd0RxTO_hjQYbdjoo"
const expectedBytes = 134217728

const defaultReadSize = 16 * 1024 * 1024


// FileReader is an io.Reader that reads a file from Google Drive sequentially.
type FileReader2 struct {
	driveApi *api.DriveApi
	id string

	// The position of this reader within the file.
	position int64

	httpResponse io.ReadCloser

}

func NewFileReader2(driveApi *api.DriveApi, id string, position int64) *FileReader2 {
	return &FileReader2{
		driveApi: driveApi,
		id: id,
		position: position,
	}
}

// Read implements the io.Reader interface.
func (f *FileReader2) Read(p []byte) (n int, err error) {
	log.Printf("FileReader2 Read of %d bytes at offset %d", len(p), f.position)

	for len(p) > 0 {
		remainingBytes := expectedBytes - f.position

		if remainingBytes == 0 {
			return 0, io.EOF
		}

		// Start a new http request if there isn't already one in progress.
		if f.httpResponse == nil {
			requestSize := min(remainingBytes, defaultReadSize)
			log.Printf("remaining %d bytes, sending request for %d bytes",
				remainingBytes, requestSize)
			resp, err := ReadAt(f.driveApi, f.id, requestSize, f.position)
			if err != nil {
				log.Printf("Error calling ReadAt: %v", err)
				// handle http 416 range not satisfiable
				break
			}
			f.httpResponse = resp
		}

		n, err = io.ReadFull(f.httpResponse, p)
		log.Printf("http request returned %d bytes: %v", n, err)

		if err == io.EOF {
			log.Printf("EOF for http request")
			f.httpResponse.Close()
			f.httpResponse = nil
		}

		// Increment the readers position in the file.
		f.position += int64(n)

		// Point p at the next available space in the buffer.
		p = p[n:]
	}

	return n, err
}

func min(a int64, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

// ReadAt ...
func ReadAt(drive *api.DriveApi, id string, size int64, off int64) (io.ReadCloser, error) {
	log.Printf("Reading %d bytes at offset %d ", size, off)
	startRange := off
	endRange := startRange + size

	request := drive.Service.Files.Get(id)
	request.Header().Add("Range", fmt.Sprintf("bytes=%d-%d", startRange, endRange))

	response, err := request.Download()
	if err != nil {
		log.Printf("Response error %v", err)
		return nil, err
	}

	return response.Body, nil
}

type TimeToFirstByteLogger struct {
	wrapped io.Writer
	start   time.Time
	Ttfb    time.Duration
}

func (t *TimeToFirstByteLogger) Write(p []byte) (n int, err error) {
	if t.Ttfb == 0 && len(p) > 0 {
		t.Ttfb = time.Since(t.start)
		log.Print("Got first byte")
	}
	return t.wrapped.Write(p)
}

func serial(drive *api.DriveApi) {
	file, err := ioutil.TempFile("", "serial")
	if err != nil {
		log.Fatal(err)
	}

	// Wrap the FileReader in a buffer so we fetch chunks of 16MiB at a time.
	reader := bufio.NewReaderSize(api.NewFileReader(drive, fileId, 0),
		defaultReadSize)

	start := time.Now()
	ttfbLogger := &TimeToFirstByteLogger{wrapped: file, start: start}
	written, err := io.Copy(ttfbLogger, reader)
	elapsed := time.Since(start)

	if written != expectedBytes {
		log.Printf("error: received wrong content length: %d", written)
	}

	mbits := (float64(written) * 8.0) / (1024.0 * 1024.0)

	log.Printf("serial: Copied %d bytes with error %v." +
		" Took %s at %0.2f mbit/s." +
		" TTFB was %s",
		written, err, elapsed, mbits / elapsed.Seconds(), ttfbLogger.Ttfb)
}

func serialStreaming(drive *api.DriveApi) {
	file, err := ioutil.TempFile("", "serial")
	if err != nil {
		log.Fatal(err)
	}

	// Wrap the FileReader in a buffer so we fetch chunks of 16MiB at a time.
	reader := NewFileReader2(drive, fileId, 0)

	start := time.Now()
	ttfbLogger := &TimeToFirstByteLogger{wrapped: file, start: start}
	written, err := io.Copy(ttfbLogger, reader)
	elapsed := time.Since(start)

	if written != expectedBytes {
		log.Printf("error: received wrong content length: %d", written)
	}

	mbits := (float64(written) * 8.0) / (1024.0 * 1024.0)

	log.Printf("serial: Copied %d bytes with error %v." +
		" Took %s at %0.2f mbit/s." +
		" TTFB was %s",
		written, err, elapsed, mbits / elapsed.Seconds(), ttfbLogger.Ttfb)
}

type ChunkReader struct {
	io.ReadCloser

	// chunk offset in the file
	offset int64
}

func parallel(drive *api.DriveApi) {
	c := make(chan int64) // The offset to read a chunk from
	reads := make(chan ChunkReader)

	// Enqueue a read request.
	c <- 0

	var wg sync.WaitGroup

	// Make 3 download workers.
	for i := 0; i < 1; i++ {
		wg.Add(1)

		// worker
		go func() {
			defer wg.Done()
			for {
				// Read a chunk request.
				offset, ok := <-c
				if !ok {
					log.Print("Channel closing")
					break // channel was closed
				}
				// download chunk at offset
				reader, err := ReadAt(drive, fileId, defaultReadSize, offset)
				if err != nil {
					log.Printf("error downloading chunk: %v", err)
					// TODO(simon): Retry with backoff.
				}

				// Request next chunk if there is one.
				if offset + defaultReadSize < expectedBytes {
					start := offset + defaultReadSize
					log.Printf("Requesting chunk beginning at %d", start)
					c <- start
				}

				// Pass this read request back so we can do something with it.
				reads <- ChunkReader{reader, offset}
			}
		}()
	}

	// Wait for all the workers to finish.
	wg.Wait()

}

func main() {
	drive := api.NewDriveApi()

	serialStreaming(drive)

	//parallel(drive)
}
