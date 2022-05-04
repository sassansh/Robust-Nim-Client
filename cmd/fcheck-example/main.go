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
	"fmt"
	"math/rand"
	"os"
	"time"

	fchecker "cs.ubc.ca/cpsc416/a2/fcheck"
)

func main() {
	// Local (127.0.0.1) hardcoded IPs to simplify testing.
	localIpPort := "127.0.0.1:8081"

	// Generate random seqNum
	rand.Seed(time.Now().UTC().UnixNano())
	randInt1 := rand.Uint32()
	rand.Seed(time.Now().UTC().UnixNano())
	randInt2 := rand.Uint32()
	var epochNonce = uint64(randInt1)<<32 + uint64(randInt2)

	// Monitor for a remote node.
	localIpPortMon := "127.0.0.1:9090"
	toMonitorIpPort := "127.0.0.1:8080"
	var lostMsgThresh uint8 = 5

	// Start fcheck. Note the use of multiple assignment:

	notifyCh, err := fchecker.Start(fchecker.StartStruct{localIpPort, epochNonce,
		localIpPortMon, toMonitorIpPort, lostMsgThresh})
	if checkError(err) != nil {
		return
	}
	fmt.Println("Started fcheck.")
	fmt.Println("Started to monitor node: ", toMonitorIpPort)

	// Wait indefinitely, blocking on the notify channel, to detect a
	// failure.
	select {
	case notify := <-notifyCh:
		fmt.Println("Detected a failure of", notify)
		return
	case <-time.After(time.Duration(int(lostMsgThresh)*20) * time.Second):
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
