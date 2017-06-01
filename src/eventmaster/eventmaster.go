package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/jessevdk/go-flags"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type dbConfig struct {
	Host          string `json:"host"`
	Port          string `json:"port"`
	Keyspace      string `json:"keyspace"`
	Consistency   string `json:"consistency"`
	ESUrl         string `json:"es_url"`
	FlushInterval int    `json:"flush_interval"`
}

func startServer(store *EventStore) {
	r := mux.NewRouter()
	eah := &eventAPIHandler{
		store: store,
	}
	tah := &topicAPIHandler{
		store: store,
	}
	dah := &dcAPIHandler{
		store: store,
	}
	r.Handle("/v1/event", eah)
	r.Handle("/v1/topic", tah)
	r.Handle("/v1/topic/{name}", tah)
	r.Handle("/v1/dc", dah)
	r.Handle("/v1/dc/{name}", dah)

	mph := &mainPageHandler{
		store: store,
	}
	cph := &createPageHandler{
		store: store,
	}
	tph := &topicPageHandler{
		store: store,
	}
	dph := &dcPageHandler{
		store: store,
	}
	r.Handle("/", mph)
	r.Handle("/add_event", cph)
	r.Handle("/topic", tph)
	r.Handle("/dc", dph)

	r.HandleFunc("/js/create_event.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "ui/js/create_event.js")
	})
	r.HandleFunc("/js/dc.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "ui/js/dc.js")
	})
	r.HandleFunc("/js/topic.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "ui/js/topic.js")
	})
	r.HandleFunc("/js/query_event.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "ui/js/query_event.js")
	})
	go func() {
		fmt.Println("http server starting on port 8080")
		http.ListenAndServe(":8080", r)
	}()
}

func main() {
	var config Config
	parser := flags.NewParser(&config, flags.Default)
	if _, err := parser.Parse(); err != nil {
		log.Fatalf("Error parsing flags: %v", err)
	}

	exp.Exp(metrics.DefaultRegistry)
	sock, err := net.Listen("tcp", "0.0.0.0:12345")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	go func() {
		fmt.Println("go-metrics server listening at port 12345")
		http.Serve(sock, nil)
	}()

	// Create listening socket
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	g := metrics.NewGauge()
	metrics.Register("goroutines", g)
	go func() {
		for {
			g.Update(int64(runtime.NumGoroutine()))
			time.Sleep(time.Duration(10) * time.Second)
		}
	}()

	store, err := NewEventStore()
	if err != nil {
		log.Fatalf("Unable to create event store: %v", err)
	}

	err = store.Update()
	if err != nil {
		fmt.Println("Error loading dcs and topics from Cassandra", err)
	}
	startServer(store)

	// Create the EventMaster server
	server, err := NewServer(&config, store)
	if err != nil {
		log.Fatalf("Unable to start server: %v", err)
	}

	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, syscall.SIGTERM, syscall.SIGINT)

	maxMsgSizeOpt := grpc.MaxMsgSize(1024 * 1024 * 100)
	// Create the gRPC server and register our service
	s := grpc.NewServer(maxMsgSizeOpt)
	eventmaster.RegisterEventMasterServer(s, server)
	reflection.Register(s)
	fmt.Println("grpc server listening on port:", config.Port)
	go func() {
		if err := s.Serve(lis); err != nil {
			// Because we graceful stop, just log this out
			// GracefulStop will kill lis, but we should not
			// throw an error to let it shut down gracefully
			fmt.Println("failed to serve:", err)

		}
	}()

	ticker := time.NewTicker(time.Second * time.Duration(store.FlushInterval))
	go func() {
		for _ = range ticker.C {
			err := store.Update()
			if err != nil {
				fmt.Println("Error updating dcs and topics from cassandra:", err)
			}
			err = store.FlushToES()
			if err != nil {
				fmt.Println("Error flushing events from temp_event to ES:", err)
			}
		}
	}()

	<-stopChan
	fmt.Println("Got shutdown signal, gracefully shutting down")
	ticker.Stop()
	store.CloseSession()
	s.GracefulStop()
}
