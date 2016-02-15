package main

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"
	"sync/atomic"
	"time"
)

var stats struct {
	messages uint64
	bytes    uint64

	prev         time.Time
	prevMessages uint64
	prevBytes    uint64
}

func IncMessage() {
	if debug {
		atomic.AddUint64(&stats.messages, 1)
	}
}

func IncBytes(written int) {
	if debug {
		atomic.AddUint64(&stats.bytes, uint64(written))
	}
}

func dcp() interface{} {
	var metrics = struct {
		Bytes         uint64
		Messages      uint64
		TotalBytes    uint64
		TotalMessages uint64
	}{}

	now := time.Now()
	metrics.TotalBytes = stats.bytes
	metrics.TotalMessages = stats.messages

	diffTime := now.Sub(stats.prev)
	diffBytes := metrics.TotalBytes - stats.prevBytes
	diffMessages := metrics.TotalMessages - stats.prevMessages

	metrics.Bytes = uint64(float64(diffBytes) / diffTime.Seconds())
	metrics.Messages = uint64(float64(diffMessages) / diffTime.Seconds())

	stats.prev = now
	stats.prevBytes = metrics.TotalBytes
	stats.prevMessages = metrics.TotalMessages

	return metrics
}

func goroutines() interface{} {
	return runtime.NumGoroutine()
}

func initDebug(mux *http.ServeMux) {
	stats.prev = time.Now()

	expvar.Publish("dcp", expvar.Func(dcp))
	expvar.Publish("goroutines", expvar.Func(goroutines))

	mux.Handle("/debug/vars/", http.HandlerFunc(expvarHandler))
	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
}
func expvarHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}
