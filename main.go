package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/websocket"

	"github.com/mattn/anko/ast"
	"github.com/mattn/anko/parser"
	"github.com/mattn/anko/vm"

	"github.com/couchbase/gocb"
	"github.com/couchbase/gocb/gocbcore"
)

const batchChunkSeparator byte = '\n'

func checkAuth(config *websocket.Config, req *http.Request) (err error) {
	_, _, ok := req.BasicAuth()
	if !ok {
		err := errors.New("missing basic auth credentials")
		log.Println(err)
		return err
	}
	return nil
}

const (
	DIRECTION_TO_CURRENT   = "to_current"
	DIRECTION_FROM_CURRENT = "from_current"
	DIRECTION_EVERYTHING   = "everything"
)

const (
	EVENT_MUTATION   = "mutation"
	EVENT_DELETION   = "deletion"
	EVENT_EXPIRATION = "expiration"
)

type Event struct {
	Type  string          `json:"event"`
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value,omitempty"`
}

func encode(packet Event) []byte {
	bytes, err := json.Marshal(&packet)
	if err != nil {
		panic(err)
	}
	return bytes
}

type State struct {
	bucket   *gocb.StreamingBucket
	ws       *websocket.Conn
	messages chan Event
	done     chan bool
	streams  map[uint16]*Stream
}

func NewState() *State {
	return &State{
		messages: make(chan Event),
		done:     make(chan bool),
		streams:  make(map[uint16]*Stream),
	}
}

func NewStateWS(ws *websocket.Conn) *State {
	state := NewState()
	state.ws = ws
	return state
}

func (s *State) NewStream(partition uint16) *Stream {
	stream := &Stream{
		partition: partition,
		state:     s,
	}
	if filterScript != nil {
		stream.env = NewAnkEnv()
		stream.script = filterScript
	}
	s.streams[partition] = stream
	return stream
}

type Stream struct {
	partition uint16
	state     *State
	env       *vm.Env
	script    []ast.Stmt
}

func (s *Stream) emit(evt Event) {
	if s.filter(evt) {
		IncMessage()
		s.state.messages <- evt
	}
}

func (s *Stream) filter(evt Event) bool {
	if s.script != nil {
		s.env.Define("event", reflect.ValueOf(evt))
		v, err := vm.Run(s.script, s.env)
		if err != nil {
			log.Fatal(err)
		}
		switch v.Kind() {
		case reflect.Bool:
			return v.Bool()
		case reflect.Interface, reflect.Ptr:
			return !v.IsNil()
		default:
			return true
		}
	}
	return true
}

func (s *Stream) SnapshotMarker(startSeqNo, endSeqNo uint64, vbId uint16, snapshotType gocbcore.SnapshotState) {
}

func (s *Stream) Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16, key, value []byte) {
	s.emit(Event{
		Type:  EVENT_MUTATION,
		Key:   string(key),
		Value: value,
	})
}

func (s *Stream) Deletion(seqNo, revNo, cas uint64, vbId uint16, key []byte) {
	s.emit(Event{
		Type: EVENT_DELETION,
		Key:  string(key),
	})
}

func (s *Stream) Expiration(seqNo, revNo, cas uint64, vbId uint16, key []byte) {
	s.emit(Event{
		Type: EVENT_EXPIRATION,
		Key:  string(key),
	})
}

func (s *Stream) End(vbId uint16, err error) {
	delete(s.state.streams, vbId)
	if len(s.state.streams) == 0 {
		s.state.done <- true
	}
}

func listenWebsocket(s *State, done chan bool) {
	s.ws.SetReadDeadline(time.Now().Add(time.Second))
	for {
		select {
		case <-done:
			return
		default:
			msg := make([]byte, 128)
			_, err := s.ws.Read(msg)
			if err == io.EOF {
				done <- true
				return
			} else if e, ok := err.(net.Error); ok {
				if e.Timeout() {
					continue
				}
			}
			if err != nil {
				log.Printf("failed to read from websocket: %v, closing streams", err)
				// TODO: do not close streams. Preserve messages and replay on reconnect
				if s.bucket != nil {
					s.bucket.IoRouter().Close()
				}
				return
			}
		}
	}
}

func writeToWebsocket(ws *websocket.Conn, bytes []byte) error {
	written, err := ws.Write(bytes)
	IncBytes(written)
	return err
}

func listenCouchbase(bucketName, password, partitions, direction string, state *State, handler func()) {
	cluster, err := gocb.Connect(clusterURI)
	if err != nil {
		log.Printf("failed to connect to cluster: %v", err)
		return
	}
	var connectionName string
	if len(partitions) > 20 {
		connectionName = fmt.Sprintf("DCPL[%d/%s]", rand.Int(), partitions[0:20])
	} else {
		connectionName = fmt.Sprintf("DCPL[%d/%s]", rand.Int(), partitions)
	}
	bucket, err := cluster.OpenStreamingBucket(connectionName, bucketName, password)
	if err != nil {
		log.Printf("failed to open the bucket %q: %v", bucketName, err)
		return
	}
	state.bucket = bucket
	type partitionState struct {
		vbUuid         gocbcore.VbUuid
		startSeqNo     gocbcore.SeqNo
		endSeqNo       gocbcore.SeqNo
		snapStartSeqNo gocbcore.SeqNo
		snapEndSeqNo   gocbcore.SeqNo
	}
	direction = parseDirection(direction)
	partitionsRange := parseRange(partitions, bucket)
	for _, p := range partitionsRange {
		pid := p
		stream := state.NewStream(p)
		ps := partitionState{
			vbUuid:         0,
			startSeqNo:     0,
			endSeqNo:       0xffffffff,
			snapStartSeqNo: 0,
			snapEndSeqNo:   0,
		}
		agent := bucket.IoRouter()
		if direction != DIRECTION_EVERYTHING {
			_, err = agent.GetFailoverLog(pid, func(flog []gocbcore.FailoverEntry, err error) {
				if err != nil {
					log.Printf("failed to get failover log for vbucket %d: %v", pid, err)
					return
				}
				ps.vbUuid = flog[0].VbUuid
				_, err = agent.GetLastCheckpoint(pid, func(lastSeqno gocbcore.SeqNo, err error) {
					if err != nil {
						log.Printf("failed to get last checkpoint for vbucket %d: %v", pid, err)
						return
					}
					switch direction {
					case DIRECTION_FROM_CURRENT:
						ps.startSeqNo = lastSeqno
						ps.endSeqNo = 0xffffffff
						ps.snapStartSeqNo = lastSeqno
						ps.snapEndSeqNo = 0xffffffff
					case DIRECTION_TO_CURRENT:
						ps.startSeqNo = 0
						ps.endSeqNo = lastSeqno
						ps.snapStartSeqNo = 0
						ps.snapEndSeqNo = lastSeqno
					}
					_, err = agent.OpenStream(pid,
						ps.vbUuid,
						ps.startSeqNo,
						ps.endSeqNo,
						ps.snapStartSeqNo,
						ps.snapEndSeqNo,
						stream, func(slog []gocbcore.FailoverEntry, err error) {
							if err != nil {
								log.Printf("failed to open DCP stream for vbucket %d: %v", pid, err)
							}
						})
					if err != nil {
						log.Printf("failed to schedule open DCP stream for vbucket %d: %v", pid, err)
						// FIXME: close opened streams or just skip failures
					}
				})
				if err != nil {
					log.Printf("failed to schedule get last checkpoint for vbucket %d: %v", pid, err)
				}
			})
			if err != nil {
				log.Printf("failed to schedule get failover log for vbucket %d: %v", pid, err)
				return
			}
		} else {
			_, err = agent.OpenStream(pid,
				ps.vbUuid,
				ps.startSeqNo,
				ps.endSeqNo,
				ps.snapStartSeqNo,
				ps.snapEndSeqNo,
				stream, func(slog []gocbcore.FailoverEntry, err error) {
					if err != nil {
						log.Printf("failed to open DCP stream for vbucket %d: %v", pid, err)
						// FIXME: close opened streams or just skip failures
						return
					}
				})
			if err != nil {
				log.Printf("failed to schedule open DCP stream for vbucket %d: %v", pid, err)
				// FIXME: close opened streams or just skip failures
				return
			}
		}
	}

	handler()
}

func onConnected(ws *websocket.Conn) {
	defer func() {
		err := ws.Close()
		if err != nil {
			log.Println("failed to close websocket")
		}
	}()

	streamState := NewStateWS(ws)
	bucket, password, _ := ws.Request().BasicAuth()
	partitions := ws.Request().Header.Get("X-Partition-Range")
	direction := ws.Request().Header.Get("X-Direction")
	done := make(chan bool)
	go listenCouchbase(bucket, password, partitions, direction, streamState,
		func() {
			var buf bytes.Buffer
			for {
				select {
				case msg := <-streamState.messages:
					p := append(encode(msg), batchChunkSeparator)
					written := false
					if buf.Len() == 0 {
						buf.Write(p)
						written = true
					}
					if buf.Len()+len(p) > batchSize {
						if err := writeToWebsocket(streamState.ws, buf.Bytes()); err != nil {
							log.Printf("failed to write to websocket: %v", err)
							return
						}
						buf.Reset()
					}
					if !written {
						buf.Write(p)
					}
				case <-streamState.done:
					done <- true
					return
				}
			}
		})
	listenWebsocket(streamState, done)
}

func parseDirection(direction string) string {
	switch strings.ToLower(strings.TrimSpace(direction)) {
	default:
		return DIRECTION_EVERYTHING
	case "from_current", "from_now":
		return DIRECTION_FROM_CURRENT
	case "to_current", "till_now":
		return DIRECTION_TO_CURRENT
	case "everything":
		return DIRECTION_EVERYTHING
	}

}

func parseRange(spec string, bucket *gocb.StreamingBucket) []uint16 {
	agent := bucket.IoRouter()
	numVbuckets := agent.NumVbuckets()
	numServers := agent.NumServers()
	partitions := make(map[int]bool)
	spec = strings.Replace(spec, " ", "", -1)
	for _, item := range strings.Split(spec, ",") {
		parts := strings.Split(item, "-")
		switch len(parts) {
		case 1:
			if strings.Index(parts[0], "auto:") == 0 {
				if idx, err := strconv.Atoi(parts[0][5:]); err == nil && idx < numServers {
					for _, p := range agent.VbucketsOnServer(idx) {
						partitions[int(p)] = true
					}
				}
			} else if p, err := strconv.Atoi(parts[0]); err == nil && p > 0 && p < math.MaxInt16 {
				partitions[p] = true
			}
		case 2:
			begin, err := strconv.Atoi(parts[0])
			if err != nil && begin <= 0 {
				continue
			}
			end, err := strconv.Atoi(parts[1])
			if err != nil && end >= math.MaxInt16 {
				continue
			}
			for p := begin; p <= end; p++ {
				partitions[p] = true
			}
		}
	}
	var keys []uint16
	if len(partitions) == 0 {
		for p := 0; p < numVbuckets; p++ {
			keys = append(keys, uint16(p))
		}
	} else {
		for p := range partitions {
			keys = append(keys, uint16(p))
		}
	}
	if debug {
		log.Printf("requested partitions: %q, expanded to: %v", spec, keys)
	}
	return keys
}

func httpHandler(w http.ResponseWriter, r *http.Request) {
	streamState := NewState()
	bucket, password, _ := r.BasicAuth()
	partitions := r.Header.Get("X-Partition-Range")
	direction := r.Header.Get("X-Direction")
	w.Header().Add("Content-Type", "application/json")
	flusher := w.(http.Flusher)
	flusher.Flush()
	listenCouchbase(bucket, password, partitions, direction, streamState,
		func() {
			for {
				select {
				case msg := <-streamState.messages:
					w.Write(append(encode(msg), batchChunkSeparator))
					flusher.Flush()
				case <-streamState.done:
					return
				}
			}
		})
}

var (
	batchSize        int
	clusterURI       string
	address          string
	debug            bool
	repl             bool
	filterScriptPath string
	filterScript     []ast.Stmt
)

func main() {
	flag.IntVar(&batchSize, "batch-size", 0, "the maximum size of websockets frame (0 - disable batching)")
	flag.StringVar(&clusterURI, "cluster-uri", "couchbase://localhost/", "URI of Couchbase cluster")
	flag.StringVar(&address, "address", "localhost:12345", "interface and port to bind the server")
	flag.StringVar(&filterScriptPath, "filter-script", "", ".ank script to filter values (bypass all event by default)")
	flag.BoolVar(&debug, "debug", false, "export pprof and expvars")
	flag.BoolVar(&repl, "repl", false, "run REPL to test and debug .ank scripts")
	flag.Parse()

	if repl {
		runREPL()
		return
	}

	log.Printf("GOMAXPROCS=%d\n", runtime.GOMAXPROCS(-1))

	if filterScriptPath != "" {
		filterScriptPath = path.Clean(filterScriptPath)
		file, err := os.Open(filterScriptPath)
		if err != nil {
			log.Fatal(err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		scanner := new(parser.Scanner)
		scanner.Init(string(content))
		filterScript, err = parser.Parse(scanner)
		if err != nil {
			if e, ok := err.(*parser.Error); ok {
				log.Fatalf("%s:%d:%d: %s\n", filterScriptPath, e.Pos.Line, e.Pos.Column, err)
			} else {
				log.Fatal(err)
			}
		}
		log.Printf("filter script: %v\n", filterScriptPath)
	}

	log.Printf("cluster URI: %v\n", clusterURI)
	log.Printf("batch size: %v bytes\n", batchSize)
	server := websocket.Server{
		Handler:   onConnected,
		Handshake: checkAuth,
	}
	mux := http.NewServeMux()
	mux.Handle("/ws/", server)
	mux.HandleFunc("/http/", httpHandler)
	log.Printf("GET /ws/            # websockets endpoint")
	log.Printf("GET /http/          # HTTP endpoint with chunked encoding")
	if debug {
		initDebug(mux)
		log.Printf("GET /debug/pprof/   # pprof reports")
		log.Printf("GET /debug/vars/    # expvar metrics")
	}
	log.Printf("listening at %s", address)
	err := http.ListenAndServe(address, mux)
	if err != nil {
		log.Fatal(err)
	}
}
