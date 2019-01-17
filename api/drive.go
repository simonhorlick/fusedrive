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
	client := getClient(config)

	srv, err := drive.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	log.Print("Got DriveApi")

	return &DriveApi{Service: srv}
}

// Retrieve a token, saves the token, then returns the generated client.
func getClient(config *oauth2.Config) *http.Client {
	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.
	tokFile := "token.json"
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokFile, tok)
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

// List returns a list of files and their ids.
// TODO(simon): Handle pagination.
func (d *DriveApi) List() []DriveApiFile {
	log.Print("DriveApi List")

	r, err := d.Service.Files.List().PageSize(10).
		Fields("nextPageToken, files(id, name)").Do()
	if err != nil {
		log.Fatalf("Unable to retrieve files: %v", err)
	}

	var files []DriveApiFile

	if len(r.Files) == 0 {
		fmt.Println("No files found.")
	} else {
		for _, i := range r.Files {
			files = append(files, DriveApiFile{
				Id: i.Id,
				Name: i.Name,
				Size: uint64(i.Size),
			})
		}
	}

	return files
}

// Upload replaces the contents of the file referenced by id with the data from
// reader.
func (d *DriveApi) Upload(id string, reader io.Reader) error {
	request := d.Service.Files.Update(id, &drive.File{
		MimeType: binaryMimeType,
	})
	request.Media(reader)

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