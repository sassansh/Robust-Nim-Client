package main

import (
	"bytes"
	fchecker "cs.ubc.ca/cpsc416/a2/fcheck"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"
)

/** Config struct **/

type ClientConfig struct {
	ClientAddress        string
	NimServerAddressList []string // Maximum 8 nim servers will be provided
	TracingServerAddress string
	Secret               []byte
	TracingIdentity      string
	// FCheck stuff:
	FCheckAckLocalAddr   string
	FCheckHbeatLocalAddr string
	FCheckLostMsgsThresh uint8
}

/** Tracing structs **/

type GameStart struct {
	Seed int8
}

type ClientMove StateMoveMessage

type ServerMoveReceive StateMoveMessage

type GameComplete struct {
	Winner string
}

/** New tracing structs introduced in A2 **/

type NewNimServer struct {
	NimServerAddress string
}

type NimServerFailed struct {
	NimServerAddress string
}

type AllNimServersDown struct {
}

/** Message structs **/

type StateMoveMessage struct {
	GameState         []uint8
	MoveRow           int8
	MoveCount         int8
	TracingServerAddr string               // ADDED IN A2
	Token             tracing.TracingToken // ADDED IN A2
}

/** Helper structs **/

type NimServer struct {
	server string
	alive  bool
}

/** Global States **/

var config *ClientConfig
var nimServers []NimServer
var currentServer = -1
var notifyChannel <-chan fchecker.FailureDetected

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: client [seed]")
		return
	}
	arg, err := strconv.Atoi(os.Args[1])
	CheckErr(err, "Provided seed could not be converted to integer", arg)
	seed := int8(arg)

	config = ReadConfig("./config/client_config.json")

	// now connect to it
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddress,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	defer tracer.Close()

	trace := tracer.CreateTrace()

	prepareNimServers(config.NimServerAddressList)

	local_ip_port := config.ClientAddress
	remote_ip_port := pickNextNimServer(trace)

	laddr, err := net.ResolveUDPAddr("udp", local_ip_port)
	CheckErr(err, "Error converting UDP address: %v\n", err)
	raddr, err := net.ResolveUDPAddr("udp", remote_ip_port)
	CheckErr(err, "Error converting UDP address: %v\n", err)

	// setup UDP connection
	conn, err := net.DialUDP("udp", laddr, raddr)
	CheckErr(err, "Couldn't connect to the server", remote_ip_port)
	defer conn.Close()

	// get board state
	sendMove := StateMoveMessage{nil, -1, seed,
		"", nil}
	var recvMove StateMoveMessage
	for {
		select {
		case <-notifyChannel:
			// Trace the failure, mark the server as dead, and close the connection
			trace.RecordAction(NimServerFailed{nimServers[currentServer].server})
			nimServers[currentServer].alive = false
			conn.Close()

			// Pick a new server
			remote_ip_port = pickNextNimServer(trace)
			raddr, err = net.ResolveUDPAddr("udp", remote_ip_port)
			CheckErr(err, "Error converting UDP address: %v\n", err)

			fmt.Println("Server Statuses: ", nimServers)

			// Try to connect to the new server
			conn, err = net.DialUDP("udp", laddr, raddr)
			CheckErr(err, "Couldn't connect to the server", remote_ip_port)
			continue
		default:
			trace.RecordAction(
				GameStart{
					Seed: seed,
				})

			// send start packet
			traceAndSend(&sendMove, trace, conn, config)
			// time.Sleep(time.Millisecond * 3000)

			// get server response
			if recvAndTrace(&recvMove, tracer, trace, conn) != nil {
				continue
			}
		}
		// A valid move was received, reset all server status to alive
		resetNimServers()
		break

	}
	state := make([]uint8, len(recvMove.GameState))
	copy(state, recvMove.GameState)

	// main loop
	for {
		// make move and update state
		sendMove = decideMove(state)
		copy(state, sendMove.GameState)
		for {
			select {
			case <-notifyChannel:
				// Trace the failure, mark the server as dead, and close the connection
				trace.RecordAction(NimServerFailed{nimServers[currentServer].server})
				nimServers[currentServer].alive = false
				conn.Close()

				// Pick a new server
				remote_ip_port = pickNextNimServer(trace)
				raddr, err = net.ResolveUDPAddr("udp", remote_ip_port)
				CheckErr(err, "Error converting UDP address: %v\n", err)

				fmt.Println("Server Statuses: ", nimServers)

				// Try to connect to the new server
				conn, err = net.DialUDP("udp", laddr, raddr)
				CheckErr(err, "Couldn't connect to the server", remote_ip_port)
				continue
			default:
				// send my move
				traceAndSend(&sendMove, trace, conn, config)

				// if I won, stop
				if isWinState(state) {
					trace.RecordAction(GameComplete{"client"})
					fchecker.Stop()
					conn.Close()
					tracer.Close()
					os.Exit(0)
				}
				// time.Sleep(time.Millisecond * 3000)

				// get server response
				if recvAndTrace(&recvMove, tracer, trace, conn) != nil {
					fmt.Fprintln(os.Stderr, "saw timeout or corrupt packet")
					// time.Sleep(time.Millisecond * 3000)
					continue
				} else if !isValidSuccessor(state, &recvMove) {
					fmt.Fprintln(os.Stderr, "saw invalid/duplicate (but not corrupt) packet")
					fmt.Fprintln(os.Stderr, "state = ", state, " received = ", recvMove.GameState)
					continue
				}
			}
			// A valid move was received, reset all server status to alive
			resetNimServers()
			break
		}
		copy(state, recvMove.GameState)
		// if server won, stop
		if isWinState(state) {
			trace.RecordAction(GameComplete{"server"})
			fchecker.Stop()
			conn.Close()
			tracer.Close()
			os.Exit(0)
		}
	}
}

func decideMove(state []uint8) StateMoveMessage {
	// winning nim strategy as described by https://en.wikipedia.org/wiki/Nim
	var nimSum uint8
	for _, elm := range state {
		nimSum ^= elm
	}

	if nimSum != 0 {
		for idx, elm := range state {
			if elm >= elm^nimSum {
				reduceBy := elm - (elm ^ nimSum)
				newState := make([]uint8, len(state))
				copy(newState, state)
				newState[idx] -= reduceBy
				return StateMoveMessage{newState, int8(idx), int8(reduceBy),
					"", nil}
			}
		}
	} else {
		for idx, elm := range state {
			if elm != 0 {
				newState := make([]uint8, len(state))
				copy(newState, state)
				newState[idx] -= 1
				return StateMoveMessage{newState, int8(idx), 1,
					"", nil}
			}
		}
	}

	fmt.Fprintln(os.Stderr, "move decision strategy failed")
	fmt.Fprintln(os.Stderr, "state = ", state)
	os.Exit(1)
	return StateMoveMessage{}
}

func isWinState(state []uint8) bool {
	for _, elm := range state {
		if elm != 0 {
			return false
		}
	}
	return true
}

func isValidSuccessor(state []uint8, move *StateMoveMessage) bool {
	for idx, elm := range state {
		if idx == int(move.MoveRow) {
			if elm-uint8(move.MoveCount) != move.GameState[idx] {
				return false
			}
		} else {
			if elm != move.GameState[idx] {
				return false
			}
		}
	}

	return true
}

func traceAndSend(move *StateMoveMessage, trace *tracing.Trace, conn net.Conn, config *ClientConfig) {
	move.TracingServerAddr = config.TracingServerAddress
	trace.RecordAction(ClientMove(*move))
	move.Token = trace.GenerateToken()
	conn.Write(encode(move))
	// assume it went through, if it didn't, we'll just retry after a timeout
}

func recvAndTrace(move *StateMoveMessage, tracer *tracing.Tracer, trace *tracing.Trace, conn net.Conn) error {
	recvBuf := make([]byte, 1024)
	timeoutPeriod := time.Duration(1) * time.Second

	conn.SetReadDeadline(time.Now().Add(timeoutPeriod))
	len, err := conn.Read(recvBuf)
	if err != nil {
		if err, ok := err.(net.Error); ok && err.Timeout() {
			// Already waited the timeout
		} else {
			// Some other error, wait the timeout period
			fmt.Println("Error reading move from nim server: ", err)
			time.Sleep(timeoutPeriod)
		}
	}
	decoded, err := decode(recvBuf, len)
	if err != nil {
		return err
	}
	*move = decoded
	tracer.ReceiveToken(move.Token)
	trace.RecordAction(ServerMoveReceive(*move))
	return nil
}

func encode(move *StateMoveMessage) []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(move)
	return buf.Bytes()
}

func decode(buf []byte, len int) (StateMoveMessage, error) {
	var decoded StateMoveMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decoded)
	if err != nil {
		return StateMoveMessage{}, err
	}
	return decoded, nil
}

func pickNextNimServer(trace *tracing.Trace) string {
	var nextServer int

	if currentServer == -1 {
		// First time picking server, select first server
		nextServer = 0
	} else if currentServer == len(nimServers)-1 {
		// Last server, select first server
		nextServer = 0
	} else {
		// Select next server
		nextServer = currentServer + 1
	}

	// Check if next server not already marked as down
	// If so, we have gone around the ring and all servers are down, exit
	if !nimServers[nextServer].alive {
		trace.RecordAction(AllNimServersDown{})
		os.Exit(0)
	}

	// Mark alive server as current and start monitoring using fcheck
	currentServer = nextServer
	trace.RecordAction(NewNimServer{nimServers[currentServer].server})
	startMonitoringNode(nimServers[currentServer].server)
	return nimServers[currentServer].server
}

func prepareNimServers(serverList []string) {
	// Check if server list is empty
	if len(serverList) == 0 {
		fmt.Fprintln(os.Stderr, "no servers to connect to")
		os.Exit(1)
	}

	// Prepare the list of servers
	for _, server := range serverList {
		nimServer := NimServer{
			server: server,
			alive:  true,
		}
		nimServers = append(nimServers, nimServer)
	}
}

func resetNimServers() {
	// Set all nim servers to alive
	for i, _ := range nimServers {
		nimServers[i].alive = true
	}
}

func startMonitoringNode(server string) {
	fchecker.Stop()
	localIpPort := config.FCheckAckLocalAddr

	// Generate random epochNonce
	rand.Seed(time.Now().UTC().UnixNano())
	randInt1 := rand.Uint32()
	rand.Seed(time.Now().UTC().UnixNano())
	randInt2 := rand.Uint32()
	epochNonce := uint64(randInt1)<<32 + uint64(randInt2)

	// Monitor for a remote node.
	localIpPortMon := config.FCheckHbeatLocalAddr
	toMonitorIpPort := server
	lostMsgThresh := config.FCheckLostMsgsThresh

	var err error

	notifyChannel, err = fchecker.Start(fchecker.StartStruct{localIpPort, epochNonce,
		localIpPortMon, toMonitorIpPort, lostMsgThresh})
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to start fcheck library: ", err)
		os.Exit(1)
	}
}

func ReadConfig(filepath string) *ClientConfig {
	configFile := filepath
	configData, err := ioutil.ReadFile(configFile)
	CheckErr(err, "reading config file")

	config := new(ClientConfig)
	err = json.Unmarshal(configData, config)
	CheckErr(err, "parsing config data")

	return config
}

func CheckErr(err error, errfmsg string, fargs ...interface{}) {
	if err != nil {
		fmt.Fprintf(os.Stderr, errfmsg, fargs...)
		os.Exit(1)
	}
}
