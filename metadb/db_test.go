package metadb

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestAttributesDoesNotExist(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestAttributesDoesNotExist")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.GetAttributes("does/not/exist")
	if err != DoesNotExist {
		t.Fatal("Expecting path to not exist")
	}
}

func TestSetAttributes(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestSetAttributes")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		log.Fatal(err)
	}

	attributes := Attributes{
		Id: "1kgcI9l0qzeB8LtmUd0RxTO_hjQYbdjoo",
		Size: 104857600,
		IsRegularFile: true,
		Mode: 0644,
	}

	err = db.SetAttributes("path/to/file", attributes)
	if err != nil {
		t.Fatal("Failed to set attributes")
	}

	var actual Attributes
	actual, err = db.GetAttributes("path/to/file")
	if err != nil {
		t.Fatal("Failed to read attributes")
	}

	if actual.Id != attributes.Id {
		t.Fatal("Id doesn't match")
	}
	if actual.Size != attributes.Size {
		t.Fatal("Size doesn't match")
	}
	if actual.IsRegularFile != attributes.IsRegularFile {
		t.Fatal("IsRegularFile doesn't match")
	}
	if actual.Mode != attributes.Mode {
		t.Fatal("Mode doesn't match")
	}
}

func TestListRootDirectory(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestListRootDirectory")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		log.Fatal(err)
	}

	attributes := Attributes{
		Id: "1kgcI9l0qzeB8LtmUd0RxTO_hjQYbdjoo",
		Size: 104857600,
		IsRegularFile: true,
		Mode: 0644,
	}
	attributes2 := Attributes{
		Id: "1vBQErMm1EY6M1Ur2C8XfrGapB6nUq1LO",
		Size: 104857600,
		IsRegularFile: true,
		Mode: 0644,
	}

	err = db.SetAttributes("a", attributes)
	if err != nil {
		t.Fatal("Failed to set attributes")
	}

	err = db.SetAttributes("a/b", attributes2)
	if err != nil {
		t.Fatal("Failed to set attributes")
	}

	var entries []Entry
	entries, err = db.List("")
	if err != nil {
		t.Fatal("Expecting directory listing")
	}
	if len(entries) != 1 {
		t.Fatal("Expecting directory listing")
	}
}

// TestListSubDirectory ensures that calling List on a subdirectory returns
// the paths relative to the subdirectory.
func TestListSubDirectory(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestListSubDirectory")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		log.Fatal(err)
	}

	attributes := Attributes{
		Id: "1kgcI9l0qzeB8LtmUd0RxTO_hjQYbdjoo",
		Size: 104857600,
		IsRegularFile: true,
		Mode: 0644,
	}
	attributes2 := Attributes{
		Id: "1vBQErMm1EY6M1Ur2C8XfrGapB6nUq1LO",
		Size: 104857600,
		IsRegularFile: true,
		Mode: 0644,
	}

	err = db.SetAttributes("a", attributes)
	if err != nil {
		t.Fatal("Failed to set attributes")
	}

	err = db.SetAttributes("a/b", attributes2)
	if err != nil {
		t.Fatal("Failed to set attributes")
	}

	var entries []Entry
	entries, err = db.List("a")
	if err != nil {
		t.Fatal("Expecting directory listing")
	}
	if len(entries) != 1 {
		t.Fatal("Expecting directory listing")
	}
	if entries[0].Path != "b" {
		t.Fatal("Expecting relative path")
	}
}

// TestListDoesntExist ensures calling List on a directory that doesn't exist
// returns the correct error code.
func TestListDoesntExist(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestListDoesntExist")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.List("a")
	if err != DoesNotExist {
		t.Fatal("Expecting directory to not exist")
	}
}

func TestSetSize(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestSetSize")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		log.Fatal(err)
	}

	attributes := Attributes{
		Id: "1kgcI9l0qzeB8LtmUd0RxTO_hjQYbdjoo",
		Size: 104857600,
		IsRegularFile: true,
		Mode: 0644,
	}

	err = db.SetAttributes("a", attributes)
	if err != nil {
		t.Fatal("Failed to set attributes")
	}

	err = db.SetSize("a", 1234)
	if err != nil {
		t.Fatal("Failed to set size")
	}

	actual, err := db.GetAttributes("a")
	if err != nil {
		t.Fatal("Failed to get attributes")
	}

	if actual.Size != 1234 {
		t.Fatal("Failed to update size")
	}
}
