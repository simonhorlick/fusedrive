// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Mounts another directory as loopback for testing and benchmarking
// purposes.

package main

import (
	"flag"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/simonhorlick/fusedrive/api"
	"log"
	"os"
	"path"
)

func main() {
	log.SetFlags(log.Lmicroseconds)
	// Scans the arg list and sets up flags
	debug := flag.Bool("debug", false, "print debugging messages.")
	other := flag.Bool("allow-other", false, "mount with -o allowother.")

	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Printf("usage: %s MOUNTPOINT\n", path.Base(os.Args[0]))
		fmt.Printf("\noptions:\n")
		flag.PrintDefaults()
		os.Exit(2)
	}

	opts := nodefs.NewOptions()

	driveApi := api.NewDriveApi()

	pathFs := pathfs.NewPathNodeFs(NewDriveFileSystem(driveApi),
		&pathfs.PathNodeFsOptions{})
	conn := nodefs.NewFileSystemConnector(pathFs.Root(), opts)
	mountPoint := flag.Arg(0)
	mOpts := &fuse.MountOptions{
		AllowOther: *other,
		Name:       "loopbackfs",
		FsName:     "drive",
		Debug:      *debug,
	}

	log.Print("Creating fuse server")

	state, err := fuse.NewServer(conn.RawFS(), mountPoint, mOpts)
	if err != nil {
		fmt.Printf("Mount fail: %v (is the mount point already in use?)\n", err)
		os.Exit(1)
	}

	fmt.Println("Mounted!")
	state.Serve()

	fmt.Println("unmounting")

	state.Unmount()
}
