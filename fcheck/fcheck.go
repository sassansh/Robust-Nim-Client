/*

This package specifies the API to the failure checking library to be
used in assignment 2 of UBC CS 416 2021W2.

You are *not* allowed to change the API below. For example, you can
modify this file by adding an implementation to Stop, but you cannot
change its API.

*/

package fchecker

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"net"
)
import "time"

////////////////////////////////////////////////////// DATA
// Define the message types fchecker has to use to communicate to other
// fchecker instances. We use Go's type declarations for this:
// https://golang.org/ref/spec#Type_declarations

// Heartbeat message.
type HBeatMessage struct {
	EpochNonce uint64 // Identifies this fchecker instance/epoch.
	SeqNum     uint64 // Unique for each heartbeat in an epoch.
}

// An ack message; response to a heartbeat.
type AckMessage struct {
	HBEatEpochNonce uint64 // Copy of what was received in the heartbeat.
	HBEatSeqNum     uint64 // Copy of what was received in the heartbeat.
}

// Notification of a failure, signal back to the client using this
// library.
type FailureDetected struct {
	UDPIpPort string    // The RemoteIP:RemotePort of the failed node.
	Timestamp time.Time // The time when the failure was detected.
}

////////////////////////////////////////////////////// API

type StartStruct struct {
	AckLocalIPAckLocalPort       string
	EpochNonce                   uint64
	HBeatLocalIPHBeatLocalPort   string
	HBeatRemoteIPHBeatRemotePort string
	LostMsgThresh                uint8
}

////////////////////////////////////////////////////// Helper Structs

type AckState struct {
	conn   *net.UDPConn
	stopCh chan string
}

type HBeatState struct {
	conn   *net.UDPConn
	stopCh chan string
}

type SentHBeat struct {
	message  HBeatMessage
	timeSent time.Time
	RTT      time.Duration
	acked    bool
}

////////////////////////////////////////////////////// Global Variables

var ackState AckState
var hBeatState HBeatState

// Starts the fcheck library.

func Start(arg StartStruct) (notifyCh <-chan FailureDetected, err error) {
	if arg.HBeatLocalIPHBeatLocalPort == "" {
		// ONLY arg.AckLocalIPAckLocalPort is set
		//
		// Start fcheck without monitoring any node, but responding to heartbeats.

		// Basic error checking
		if arg.AckLocalIPAckLocalPort == "" {
			return nil, errors.New("no AckLocalIPAckLocalPort specified")
		}
		if ackState.conn != nil {
			return nil, errors.New("already acknowledging heartbeats on: " +
				ackState.conn.LocalAddr().String() + " call Stop() first")
		}

		// Parse local IP:port to ack from
		laddrAck, err := net.ResolveUDPAddr("udp", arg.AckLocalIPAckLocalPort)
		if err != nil {
			return nil, err
		}

		// Create a UDP connection to send acks from
		connAck, err := net.ListenUDP("udp", laddrAck)
		if err != nil {
			return nil, err
		}

		// Start responding to heartbeats
		ackState.conn = connAck
		ackState.stopCh = make(chan string)
		go startAcking()

		fmt.Println("Started fchecker without monitoring any node, but responding to heartbeats on: " +
			connAck.LocalAddr().String())

		return nil, nil
	}
	// Else: ALL fields in arg are set
	// Start the fcheck library by monitoring a single node and
	// also responding to heartbeats.

	// Basic error checking
	if arg.LostMsgThresh < 1 {
		return nil, errors.New("LostMsgThresh must be >= 1")
	}
	if arg.EpochNonce == 0 {
		return nil, errors.New("EpochNonce must be non-zero")
	}
	if hBeatState.conn != nil {
		return nil, errors.New("already monitoring a node: " +
			hBeatState.conn.RemoteAddr().String() + ", call Stop() first")
	}
	if ackState.conn != nil {
		return nil, errors.New("fcheck already started! Acknowledging heartbeats on: " +
			ackState.conn.LocalAddr().String() + ", call Stop() first")
	}

	// Parse local IP:port to ack from
	laddrAck, err := net.ResolveUDPAddr("udp", arg.AckLocalIPAckLocalPort)
	if err != nil {
		return nil, err
	}

	// Create a UDP connection to send acks from
	connAck, err := net.ListenUDP("udp", laddrAck)
	if err != nil {
		return nil, err
	}

	// Start responding to heartbeats
	ackState.conn = connAck
	ackState.stopCh = make(chan string)
	go startAcking()

	// Parse local IP:port to send heartbeats from
	laddr, err := net.ResolveUDPAddr("udp", arg.HBeatLocalIPHBeatLocalPort)
	if err != nil {
		return nil, err
	}

	// Parse remote node IP:port to send heartbeats to
	raddr, err := net.ResolveUDPAddr("udp", arg.HBeatRemoteIPHBeatRemotePort)
	if err != nil {
		return nil, err
	}

	// Create a UDP connection to send heartbeats to
	connHBeat, err := net.DialUDP("udp", laddr, raddr)
	if err != nil {
		return nil, err
	}

	// Create a channel to receive notifications of failures
	notifyChannel := make(chan FailureDetected)

	// Start monitoring the remote node
	hBeatState.conn = connHBeat
	hBeatState.stopCh = make(chan string)
	go monitorNode(notifyChannel, arg.EpochNonce, arg.LostMsgThresh)

	fmt.Println("fcheck Started!")
	fmt.Println("Responding to heartbeats on: " +
		connAck.LocalAddr().String())
	fmt.Println("Monitoring node  " +
		connHBeat.RemoteAddr().String() + " from " +
		connHBeat.LocalAddr().String())

	return notifyChannel, nil
}

func monitorNode(notifyCh chan FailureDetected, epochNonce uint64, lostMsgThresh uint8) {
	averageRTT := time.Millisecond * 3000
	lostMsgCount := uint8(0)
	var lastSentHBeatTime time.Time
	HBeatsSent := make([]SentHBeat, 0)

	// Generate random seqNum
	rand.Seed(time.Now().UTC().UnixNano())
	randInt1 := rand.Uint32()
	rand.Seed(time.Now().UTC().UnixNano())
	randInt2 := rand.Uint32()
	seqNum := uint64(randInt1)<<32 + uint64(randInt2)
	fmt.Println("Average RTT:", averageRTT)

	for {
		select {
		case <-hBeatState.stopCh:
			return
		default:
			// Check for timeout
			timeoutOccurred := false
			if len(HBeatsSent) > 0 && time.Since(lastSentHBeatTime) > averageRTT {
				timeoutOccurred = true
				lostMsgCount++

				fmt.Println("Timeout occurred, lostMsgCount:", lostMsgCount)

				// If too many messages have been lost, send a failure notification and stop monitoring
				if lostMsgCount >= lostMsgThresh {
					fmt.Println("LostMsgCount(", lostMsgCount, ") >= LostMsgThresh(", lostMsgThresh,
						"), sending failure notification")
					notifyCh <- FailureDetected{
						UDPIpPort: hBeatState.conn.RemoteAddr().String(),
						Timestamp: time.Now(),
					}
					remoteNodeAddr := hBeatState.conn.RemoteAddr().String()
					close(hBeatState.stopCh)
					hBeatState.conn.Close()
					hBeatState.conn = nil
					fmt.Println("Stopped monitoring remote node: ", remoteNodeAddr)
					return
				}
			}

			// Check for heartbeat response
			matchingHBeatFound := false
			if len(HBeatsSent) > 0 {
				recvBuf := make([]byte, 1024)
				hBeatState.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
				len, err := hBeatState.conn.Read(recvBuf)
				if err == nil {
					// Decode the response
					receivedAck, err := decodeAck(recvBuf, len)
					if err != nil {
						fmt.Println("Error decoding AckMessage:", err)
						if timeoutOccurred {
							lostMsgCount--
						}
						continue
					}

					// Check EpochNonce matches
					if receivedAck.HBEatEpochNonce == epochNonce {
						// Find matching sent heartbeat and update RTT
						for _, hb := range HBeatsSent {
							if hb.message.SeqNum == receivedAck.HBEatSeqNum {
								fmt.Println(time.Now().Format("15:04:05.000000"), "<-- Received Ack:",
									receivedAck, "from", hBeatState.conn.RemoteAddr())
								matchingHBeatFound = true
								lostMsgCount = 0

								// Update RTT if first ack received for this Heartbeat
								if !hb.acked {
									fmt.Println("Updating RTT: (", averageRTT, " + ", time.Since(hb.timeSent),
										") / 2 = ", (averageRTT+time.Since(hb.timeSent))/2)
									averageRTT = (averageRTT + time.Since(hb.timeSent)) / 2
								}
								hb.acked = true

								// If response came early, sleep for the difference
								ExtraSleepTime := time.Until(lastSentHBeatTime.Add(hb.RTT)) - time.Millisecond*50
								if ExtraSleepTime > 0 {
									time.Sleep(ExtraSleepTime)
								}
							}
						}
					}
					// No matching heartbeat found
					if !matchingHBeatFound {
						fmt.Println("Received unexpected AckMessage:", receivedAck)
					}
				}
			}

			// Send a heartbeat if timeout, no matching heartbeat found, or heartbeat never sent,
			// meaning no heartbeat on the wire
			if timeoutOccurred || matchingHBeatFound || len(HBeatsSent) == 0 {
				// Create a new heartbeat
				HBeatMessage := HBeatMessage{
					EpochNonce: epochNonce,
					SeqNum:     seqNum,
				}
				// Send the heartbeat
				_, err := hBeatState.conn.Write(encodeHBeat(HBeatMessage))
				if err != nil {
					fmt.Println("Error sending heartbeat:", err)
					if timeoutOccurred {
						lostMsgCount--
					}
					continue
				}
				// Record heartbeat in array
				lastSentHBeatTime = time.Now()
				hBeatSent := SentHBeat{
					message:  HBeatMessage,
					timeSent: time.Now(),
					RTT:      averageRTT,
				}
				HBeatsSent = append(HBeatsSent, hBeatSent)
				fmt.Println(hBeatSent.timeSent.Format("15:04:05.000000"), "--> Sent Heartbeat", HBeatMessage,
					"to", hBeatState.conn.RemoteAddr())

				seqNum++
			}
		}
	}
}

func startAcking() {
	recvBuf := make([]byte, 1024)

	for {
		select {
		case <-ackState.stopCh:
			return
		default:
			// Receive a heartbeat
			ackState.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100)) // TODO: What timeout to use?
			len, raddr, err := ackState.conn.ReadFromUDP(recvBuf)
			if err != nil {
				if err, ok := err.(net.Error); ok && err.Timeout() {
					// Timeout, continue
					continue
				} else {
					// Some other error
					fmt.Println("Error reading HeartBeat from UDP connection:", err)
					continue
				}
			}

			// Decode the heartbeat
			receivedHBeat, err := decodeHBeat(recvBuf, len)
			if err != nil {
				fmt.Println("Error decoding HeartBeat:", err)
				continue
			}
			fmt.Println(time.Now().Format("15:04:05.000000"), "<-- Received HeartBeat:",
				receivedHBeat, "from", raddr)

			// Send an ack
			ackMsg := AckMessage{
				HBEatEpochNonce: receivedHBeat.EpochNonce,
				HBEatSeqNum:     receivedHBeat.SeqNum,
			}
			_, err = ackState.conn.WriteTo(encodeAck(ackMsg), raddr)
			if err != nil {
				fmt.Println("Error writing Ack to UDP connection:", err)
				continue
			}
			fmt.Println(time.Now().Format("15:04:05.000000"), "--> Sent Ack", ackMsg,
				"to", raddr)
		}
	}
}

////////////////////////////////////////////////////// Acking Helper Functions

func decodeHBeat(buf []byte, len int) (HBeatMessage, error) {
	var decodedHBeat HBeatMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decodedHBeat)
	if err != nil {
		return HBeatMessage{}, err
	}
	return decodedHBeat, nil
}

func encodeAck(ackMessage AckMessage) []byte {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(ackMessage)
	if err != nil {
		fmt.Println("Error encoding AckMessage:", err)
		return nil
	}
	return buf.Bytes()
}

////////////////////////////////////////////////////// HBeat Helper Functions

func encodeHBeat(HBMessage HBeatMessage) []byte {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(HBMessage)
	if err != nil {
		fmt.Println("Error encoding HBeatMessage:", err)
		return nil
	}
	return buf.Bytes()
}

func decodeAck(buf []byte, len int) (AckMessage, error) {
	var decodedAck AckMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decodedAck)
	if err != nil {
		return AckMessage{}, err
	}
	return decodedAck, nil
}

// Tells the library to stop monitoring/responding acks.
func Stop() {
	// Stop responding to heartbeats
	if ackState.conn != nil {
		heartBeatRespondAddr := ackState.conn.LocalAddr().String()
		ackState.stopCh <- "stop"
		close(ackState.stopCh)
		ackState.conn.Close()
		ackState.conn = nil
		fmt.Println("Stopped responding to heartbeats on: ", heartBeatRespondAddr)
	}

	// Stop monitoring remote node
	if hBeatState.conn != nil {
		remoteNodeAddr := hBeatState.conn.RemoteAddr().String()
		hBeatState.stopCh <- "stop"
		close(hBeatState.stopCh)
		hBeatState.conn.Close()
		hBeatState.conn = nil
		fmt.Println("Stopped monitoring remote node: ", remoteNodeAddr)
	}

	return
}
