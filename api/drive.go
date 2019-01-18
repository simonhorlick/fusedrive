package api

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
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
	Id   string

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

// Create uploads a new file to the remote and returns the id of the created
// file.
func (d *DriveApi) Create(reader io.Reader) (string, error) {
	// TODO(simon): Add retries and exponential backoff.

	request := d.Service.Files.Create(&drive.File{
		MimeType: binaryMimeType,
	}).Media(reader)

	log.Printf("Creating file")
	start := time.Now()
	file, err := request.Do()
	if err != nil {
		log.Printf("Error creating file, err: %#v, %v", file, err)
		return "", err
	}
	log.Printf("Uploaded file %s in %.03f seconds", file.Id,
		time.Since(start).Seconds())

	return file.Id, nil
}

// Update replaces the contents of the given file with the data from reader.
func (d *DriveApi) Update(id string, reader io.Reader) error {
	// TODO(simon): Add retries and exponential backoff.

	request := d.Service.Files.Update(id, &drive.File{
		MimeType: binaryMimeType,
	}).Media(reader)

	log.Printf("Uploading file %s", id)
	start := time.Now()
	file, err := request.Do()
	if err != nil {
		log.Printf("Error uploading file, err: %#v, %v", file, err)
		return err
	}
	log.Printf("Uploaded file %s in %.03f seconds", id,
		time.Since(start).Seconds())

	return nil
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

	request := d.Service.Files.Get(id)
	request.Header().Add("Range",
		fmt.Sprintf("bytes=%d-%d", startRange, endRange))

	response, err := request.Download()
	if err != nil {
		log.Printf("Response error %v", err)
		return nil, err
	}

	return response.Body, nil
}
