package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

type DriveApi struct {
	Service *drive.Service
}

type DriveApiFile struct {
	Name string
	Id string
}

func NewDriveApi() *DriveApi {
	log.Print("Reading credentials")
	b, err := ioutil.ReadFile("credentials.json")
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

	fmt.Println("Files:")
	if len(r.Files) == 0 {
		fmt.Println("No files found.")
	} else {
		for _, i := range r.Files {
			fmt.Printf("%s (%s)\n", i.Name, i.Id)
			files = append(files, DriveApiFile{
				Id: i.Id,
				Name: i.Name,
			})
		}
	}

	return files
}

// Read downloads and returns the contents of the entire file in p.
func (d *DriveApi) Read(id string, p []byte) (n int, err error) {
	log.Print("DriveApiFile Read")

	response, err := d.Service.Files.Get(id).Download()
	if err != nil {
		return 0, err
	}

	defer response.Body.Close()
	written, err := io.Copy(bytes.NewBuffer(p), response.Body)
	return int(written), err
}

// ReadAt downloads and returns the contents of the file within the specified
// range and places it in p.
func (d *DriveApi) ReadAt(id string, p []byte, off int64) (n int, err error) {
	startRange := off
	endRange := startRange + int64(len(p))

	request := d.Service.Files.Get(id)
	request.Header().Add("Range", fmt.Sprintf("bytes=%d-%d", startRange, endRange))

	response, err := request.Download()
	if err != nil {
		log.Printf("Response error %v", err)
		return 0, err
	}

	defer response.Body.Close()

	written, err := io.ReadFull(response.Body, p)
	return int(written), err
}

const fieldsToReturn = "id,name,size,md5Checksum,trashed,modifiedTime,createdTime,parents,mimeType"

// GetAttr returns the metadata for the file.
func (d *DriveApi) GetAttr(id string) (*drive.File, error) {
	log.Printf("DriveApiFile GetAttr (%s)", id)
	return d.Service.Files.Get(id).Fields(fieldsToReturn).Do()
}

// Create makes a new file.
func (d *DriveApi) Create() {
	log.Print("DriveApiFile Create")
	// TODO(simon): Implement
}

// Write replaces the file in its entirety.
func (d *DriveApi) Write() {
	log.Print("DriveApiFile Write")

	// TODO(simon): Implement
}

func (d *DriveApi) GetByName(s string) *DriveApiFile {
	log.Printf("DriveApiFile GetByName %s", s)

	// TODO(simon): Is this the best way? Seems inefficient.
	for _, file := range d.List() {
		if file.Name == s {
			return &file
		}
	}

	return nil
}
