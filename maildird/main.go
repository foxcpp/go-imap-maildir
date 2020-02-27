package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/emersion/go-imap/server"
	"github.com/foxcpp/go-imap-maildir"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "maildird - Dumb IMAP4rev1 server providing unauthenticated access a set of maildirs\n")
		fmt.Fprintf(os.Stderr, "Usage: %s <path template> <endpoint>\n", os.Args[0])
		os.Exit(2)
	}

	pathTemplate := os.Args[1]
	endpoint := os.Args[2]

	bkd, err := imapmaildir.New(pathTemplate)
	bkd.Debug = log.New(os.Stderr, "imapmaildir[debug]: ", 0)
	defer bkd.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Backend initialization failed: %v\n", err)
		os.Exit(2)
	}

	srv := server.New(bkd)
	defer srv.Close()

	srv.AllowInsecureAuth = true

	l, err := net.Listen("tcp", endpoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	go func() {
		if err := srv.Serve(l); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	<-sig
}
