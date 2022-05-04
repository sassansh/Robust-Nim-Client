/*

A trivial application to illustrate how the fcheck library can be used
in assignment 2 for UBC CS 416 2021W2.

Usage:
	go run cmd/fcheck-example/main.go
or:
	make example
	./bin/example
*/

package main

// Expects fcheck.go to be in the ./fcheck/ dir, relative to
// this fcheck-client.go file
import (
	fchecker "cs.ubc.ca/cpsc416/a2/fcheck"
	"fmt"
	"os"
	"time"
)

func main() {
	// Local (127.0.0.1) hardcoded IPs to simplify testing.
	localIpPort := "127.0.0.1:8080"

	// Start fcheck. Note the use of multiple assignment:

	_, err := fchecker.Start(fchecker.StartStruct{localIpPort, 0,
		"", "", 0})
	if checkError(err) != nil {
		return
	}
	fmt.Println("Started fcheck.")
	fmt.Println("Started acking heartbeats on: ", localIpPort)

	// Respond to heartbeats, wait for fcheck to stop.
	select {
	case <-time.After(100000 * time.Second):
		// case <-time.After(time.Second):
		fchecker.Stop()
	}
}

// If error is non-nil, print it out and return it.
func checkError(err error) error {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}
