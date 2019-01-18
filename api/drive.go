package api

import (
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"

	"github.com/cenkalti/backoff"
)

const (
	// credentialsFileName is the filename where we expect to find a credentials
	// file.
	credentialsFileName = "credentials.json"

	// tokenFileName is where we expect to find a refresh token file.
	tokenFileName = "token.json"

	// binaryMimeType is the value of the MimeType attribute that is set on
	// files uploaded to Google Drive.
	binaryMimeType = "application/octet-stream"
)

type DriveApi struct {
	Service *drive.Service
}

type DriveApiFile struct {
	// The absolute path name that this file belongs to.
	Name string

	// The Google Drive id of this file.
	Id string

	// The size of this file in bytes.
	Size uint64
}

func NewDriveApi(dataPath string) *DriveApi {
	credentialsFile := path.Join(dataPath, credentialsFileName)
	log.Printf("Reading credentials from %s", credentialsFile)
	b, err := ioutil.ReadFile(credentialsFile)
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// Request read/write access.
	config, err := google.ConfigFromJSON(b, drive.DriveFileScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(config, dataPath)

	srv, err := drive.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	return &DriveApi{Service: srv}
}

// Retrieve a token, saves the token, then returns the generated client.
func getClient(config *oauth2.Config, dataPath string) *http.Client {
	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.
	tokenFile := path.Join(dataPath, tokenFileName)

	tok, err := tokenFromFile(tokenFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokenFile, tok)
	}
	return config.Client(context.Background(), tok)
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

// isHttpSuccess returns true if this status code signals success.
func isHttpSuccess(code int) bool {
	return code >= 200 && code < 300
}

// Create uploads a new file to the remote and returns the id of the created
// file.
func (d *DriveApi) Create(reader io.Reader) (string, error) {
	// TODO(simon): Log progress of uploads.
	var response *drive.File
	call := func() error {
		request := d.Service.Files.Create(&drive.File{
			MimeType: binaryMimeType,
		}).Media(reader)

		log.Printf("Calling Files.Create")
		var err error
		response, err = request.Do()

		log.Printf("Files.Create returned %#v", response)

		// Either response is nil, or error is nil.
		if err != nil {
			log.Printf("Files.Create response error for: %v", err)
			return err
		} else if !isHttpSuccess(response.HTTPStatusCode) {
			// Determine whether the request will eventually succeed if we keep
			// retrying.
			if IsPermanentError(response.HTTPStatusCode) {
				log.Printf("Files.Create request cannot be retried: %v", err)
				return backoff.Permanent(err)
			} else {
				return err
			}
		}

		// Success.
		return nil
	}

	// Keep attempting the call until it succeeds, or we fail with a permanent
	// error.
	err := backoff.Retry(call, backoff.NewExponentialBackOff())
	if err != nil {
		return "", err
	}

	return response.Id, nil
}

// Update replaces the contents of the given file with the data from reader.
func (d *DriveApi) Update(id string, reader io.Reader) error {
	// TODO(simon): Log progress of uploads.
	call := func() error {
		request := d.Service.Files.Update(id, &drive.File{
			MimeType: binaryMimeType,
		}).Media(reader)

		log.Printf("Calling Files.Update for %s", id)
		response, err := request.Do()

		log.Printf("Files.Update returned %#v for %s", response, id)

		if err != nil {
			log.Printf("Files.Update response error for %s: %v", id, err)
			return err
		} else if !isHttpSuccess(response.HTTPStatusCode) {
			// Determine whether the request will eventually succeed if we keep
			// retrying.
			if IsPermanentError(response.HTTPStatusCode) {
				log.Printf("Files.Update for %s request cannot be retried: %v",
					id, err)
				return backoff.Permanent(err)
			} else {
				return err
			}
		}

		// Success.
		return nil
	}

	// Keep attempting the call until it succeeds, or we fail with a permanent
	// error.
	return backoff.Retry(call, backoff.NewExponentialBackOff())
}

// ReadAt returns the content of the file in the given range with the given
// id.
func (d *DriveApi) ReadAt(id string, size uint64, off uint64) (io.ReadCloser,
	error) {
	if size == 0 {
		log.Printf("error: Attempted zero byte read")
		return nil, nil
	}

	// The byte range specified in the Range header is [start,end] inclusive. So
	// [0,1023] would return 1024 bytes.
	startRange := off
	endRange := startRange + size - 1

	var response *http.Response
	call := func() error {
		request := d.Service.Files.Get(id)
		request.Header().Add("Range",
			fmt.Sprintf("bytes=%d-%d", startRange, endRange))

		log.Printf("Calling Files.Get for %s", id)
		var err error
		response, err = request.Download()

		log.Printf("Files.Get returned %#v for %s", response, id)

		if err != nil {
			log.Printf("Files.Get response error for %s: %v", id, err)
			return err
		} else if !isHttpSuccess(response.StatusCode) {
			// Determine whether the request will eventually succeed if we keep
			// retrying.
			if IsPermanentError(response.StatusCode) {
				log.Printf("Files.Get request for %s cannot be retried: %v", id,
					err)
				return backoff.Permanent(err)
			} else {
				return err
			}
		}

		// Success.
		return nil
	}

	// Keep attempting the call until it succeeds, or we fail with a permanent
	// error.
	err := backoff.Retry(call, backoff.NewExponentialBackOff())
	if err != nil {
		return nil, err
	}

	return response.Body, nil
}

func (d *DriveApi) ReadAll(id string, file *os.File) error {
	call := func() error {
		log.Printf("Calling Files.Get for %s", id)
		response, err := d.Service.Files.Get(id).Download()

		log.Printf("Files.Get returned %s for %s", response.Status, id)

		if err != nil {
			log.Printf("Files.Get response error for %s: %v", id, err)
			return err
		} else if !isHttpSuccess(response.StatusCode) {
			// Determine whether the request will eventually succeed if we keep
			// retrying.
			if IsPermanentError(response.StatusCode) {
				log.Printf("Files.Get request for %s cannot be retried: %v", id,
					err)
				return backoff.Permanent(err)
			} else {
				return err
			}
		}

		n, err := io.Copy(file, response.Body)
		if err != nil {
			log.Printf("Files.Get error reading response for %s: %v", id, err)
			return err
		}
		log.Printf("Files.Get returned %d bytes for %s", n, id)

		// Success.
		return nil
	}

	// Keep attempting the call until it succeeds, or we fail with a permanent
	// error.
	err := backoff.Retry(call, backoff.NewExponentialBackOff())
	if err != nil {
		return err
	}

	return nil

}

// IsPermanentError returns true if the request should not be retried.
func IsPermanentError(status int) bool {
	switch status {
	// This can mean that a required field or parameter has not been
	// provided, the value supplied is invalid, or the combination of
	// provided fields is invalid.
	case http.StatusBadRequest:
		return true

	// TODO(simon): Attempt to refresh the access token.
	case http.StatusUnauthorized:
		return true

	// This is likely a rate limit. Retry with backoff.
	case http.StatusForbidden:
		return false

	// The user does not have read access to a file, or the file does not
	// exist.
	case http.StatusNotFound:
		return true

	case http.StatusTooManyRequests:
		return false

	// Catch-all error, retry with backoff.
	case http.StatusInternalServerError:
		return false

	// For unknown responses, do not retry.
	default:
		log.Printf("Unknown http response status: %v", status)
		return true
	}
}
