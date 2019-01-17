package main

import (
	"flag"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/simonhorlick/fusedrive/api"
	"github.com/simonhorlick/fusedrive/metadb"
	"log"
	"os"
	"path"
)

func main() {
	log.SetFlags(log.Lmicroseconds)
	// Scans the arg list and sets up flags
	debug := flag.Bool("debug", false, "print debugging messages.")
	other := flag.Bool("allow-other", false, "mount with -o allowother.")
	dataDir := flag.String("datadir", "/var/fusedrive",
		"directory to store meta database and credentials file")

	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Printf("usage: %s MOUNTPOINT\n", path.Base(os.Args[0]))
		fmt.Printf("\noptions:\n")
		flag.PrintDefaults()
		os.Exit(2)
	}

	opts := nodefs.NewOptions()

	driveApi := api.NewDriveApi(*dataDir)

	db, err := metadb.Open(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	syncer := api.NewSyncer(db, driveApi)
	go syncer.Start()

	pathFs := pathfs.NewPathNodeFs(NewDriveFileSystem(driveApi, db, syncer),
		&pathfs.PathNodeFsOptions{})
	conn := nodefs.NewFileSystemConnector(pathFs.Root(), opts)
	mountPoint := flag.Arg(0)
	mOpts := &fuse.MountOptions{
		AllowOther: *other,
		Name:       "fusedrive",
		FsName:     "drive",
		Debug:      *debug,
		MaxWrite:   fuse.MAX_KERNEL_WRITE,
		Options: []string{
			fmt.Sprintf("max_read=%d", fuse.MAX_KERNEL_WRITE),
		},
	}

	log.Print("Creating fuse server")

	state, err := fuse.NewServer(conn.RawFS(), mountPoint, mOpts)
	if err != nil {
		log.Fatalf("Mount fail: %v (is the mount point already in use?)\n", err)
	}

	fmt.Println("Mounted!")
	state.Serve()

	fmt.Println("unmounting")

	state.Unmount()
}
