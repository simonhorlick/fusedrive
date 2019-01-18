package api

import (
	"bufio"
	"github.com/simonhorlick/fusedrive/metadb"
	"io"
	"log"
	"os"
)

// Syncer implements an upload queue for writing files to Google Drive.
type Syncer struct {
	// db stores the current upload queue
	db *metadb.DB

	queue chan metadb.Upload

	// quit is a channel that stops the syncer when an element is added
	quit chan interface{}

	// remote is where we are uploading files to
	remote Remote
}

func NewSyncer(db *metadb.DB, remote Remote) *Syncer {
	uploadQueue := make(chan metadb.Upload, 10)

	// Read queue from database and resume uploading from where we left off.
	for _, upload := range db.GetUploadQueue() {
		uploadQueue <- upload
	}

	return &Syncer{
		db:	   db,
		queue: uploadQueue,
		quit:  make(chan interface{}),
		remote: remote,
	}
}

// Start uploading files from the queue. Must be run as a goroutine.
func (s *Syncer) Start() error {
	log.Printf("Starting syncer")

	for {
		select {
		case <- s.quit:
			log.Printf("Shutting down syncer")
			return nil
		case upload := <-s.queue:
			log.Printf("Read %s off queue", upload.Path)
			err := s.uploadFile(upload)
			if err != nil {
				log.Printf("error uploading file %s: %v", upload.Path, err)
				// TODO(simon): Add retries
				continue
			}
			log.Printf("Removing %s from cache", upload.Path)
			s.db.RemoveFromUploadQueue(upload)
		}
	}
}

// Stop shuts down the Syncer after the current upload has completed.
func (s *Syncer) Stop() {
	s.quit <- struct{}{}
}

// EnqueueFile takes the path to a file on the filesystem and enqueues it for
// upload.
func (s *Syncer) EnqueueFile(id, path string) error {
	log.Printf("Syncer UploadFile %s", path)

	upload := metadb.Upload{Id: id, Path: path}

	// Persist the path in the db so if this process dies we can retry uploading
	// it later.
	err := s.db.AddToUploadQueue(upload)
	if err != nil {
		return err
	}

	log.Printf("enqueue")

	// Notify that there's a new file to upload.
	s.queue <- upload

	log.Printf("enqueued")

	return nil
}

// uploadFile attempts to upload the given file.
func (s *Syncer) uploadFile(upload metadb.Upload) error {
	file, err := os.Open(upload.Path)
	if err != nil {
		return err
	}

	err = s.remote.Upload(upload.Id, bufio.NewReader(file))
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	// TODO(simon): Only remove files once the cache is out of space.
	err = os.Remove(upload.Path)
	if err != nil {
		return err
	}

	return nil
}
