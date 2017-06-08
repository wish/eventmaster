package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
	log "github.com/Sirupsen/logrus"
	"github.com/jessevdk/go-flags"
	"github.com/julienschmidt/httprouter"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

type dbConfig struct {
	CassandraAddr  string `json:"cassandra_addr"`
	Keyspace       string `json:"keyspace"`
	Consistency    string `json:"consistency"`
	ESAddr         string `json:"es_addr"`
	FlushInterval  int    `json:"flush_interval"`
	UpdateInterval int    `json:"update_interval"`
}

func getConfig() dbConfig {
	dbConf := dbConfig{}
	confFile, err := ioutil.ReadFile("db_config.json")
	if err != nil {
		fmt.Println("No db_config file specified")
	} else {
		err = json.Unmarshal(confFile, &dbConf)
		if err != nil {
			fmt.Println("Error parsing db_config.json, using defaults:", err)
		}
	}
	if dbConf.CassandraAddr == "" {
		dbConf.CassandraAddr = "127.0.0.1:9042"
	}
	if dbConf.Keyspace == "" {
		dbConf.Keyspace = "event_master"
	}
	if dbConf.Consistency == "" {
		dbConf.Consistency = "quorum"
	}
	if dbConf.ESAddr == "" {
		dbConf.ESAddr = "http://127.0.0.1:9200"
	} else if !strings.HasPrefix(dbConf.ESAddr, "http://") {
		dbConf.ESAddr = "http://" + dbConf.ESAddr
	}
	if dbConf.FlushInterval == 0 {
		dbConf.FlushInterval = 5
	}
	if dbConf.UpdateInterval == 0 {
		dbConf.UpdateInterval = 5
	}
	return dbConf
}

func getHTTPServer(store *EventStore, registry metrics.Registry) *http.Server {
	r := httprouter.New()
	h := httpHandler{
		store: store,
	}

	r.POST("/v1/event", wrapHandler(h.handleAddEvent, registry))
	r.GET("/v1/event", wrapHandler(h.handleGetEvent, registry))
	r.POST("/v1/topic", wrapHandler(h.handleAddTopic, registry))
	r.PUT("/v1/topic/:name", wrapHandler(h.handleUpdateTopic, registry))
	r.GET("/v1/topic", wrapHandler(h.handleGetTopic, registry))
	r.DELETE("/v1/topic", wrapHandler(h.handleDeleteTopic, registry))
	r.POST("/v1/dc", wrapHandler(h.handleAddDc, registry))
	r.PUT("/v1/dc/:name", wrapHandler(h.handleUpdateDc, registry))
	r.GET("/v1/dc/:name", wrapHandler(h.handleGetDc, registry))

	r.GET("/", HandleMainPage)
	r.GET("/add_event", HandleCreatePage)
	r.GET("/topic", HandleTopicPage)
	r.GET("/dc", HandleDcPage)

	r.ServeFiles("/js/*filepath", http.Dir("ui/js"))

	return &http.Server{
		Handler: r,
	}
}

func main() {
	var config Config
	parser := flags.NewParser(&config, flags.Default)
	if _, err := parser.Parse(); err != nil {
		log.Fatalf("Error parsing flags: %v", err)
	}

	r := metrics.NewRegistry()

	// Set up event store
	dbConf := getConfig()
	store, err := NewEventStore(dbConf, r)
	if err != nil {
		log.Fatalf("Unable to create event store: %v", err)
	}
	if err := store.Update(); err != nil {
		fmt.Println("Error loading dcs and topics from Cassandra", err)
	}

	// Create listening socket for grpc server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	mux := cmux.New(lis)
	httpL := mux.Match(cmux.HTTP1Fast())
	grpcL := mux.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))

	httpS := getHTTPServer(store, r)

	// Create the EventMaster server
	grpcServer, err := NewGRPCServer(&config, store, r)
	if err != nil {
		log.Fatalf("Unable to start server: %v", err)
	}

	maxMsgSizeOpt := grpc.MaxMsgSize(1024 * 1024 * 100)
	// Create the gRPC server and register our service
	grpcS := grpc.NewServer(maxMsgSizeOpt)
	eventmaster.RegisterEventMasterServer(grpcS, grpcServer)

	go httpS.Serve(httpL)
	go grpcS.Serve(grpcL)

	go func() {
		fmt.Println("Starting server on port", config.Port)
		if err := mux.Serve(); err != nil {
			log.Fatalf("Error starting server: %v", err)
		}
	}()

	flushTicker := time.NewTicker(time.Second * time.Duration(dbConf.FlushInterval))
	go func() {
		for range flushTicker.C {
			if err := store.FlushToES(); err != nil {
				fmt.Println("Error flushing events from temp_event to ES:", err)
			}
		}
	}()

	updateTicker := time.NewTicker(time.Second * time.Duration(dbConf.UpdateInterval))
	go func() {
		for range updateTicker.C {
			if err := store.Update(); err != nil {
				fmt.Println("Error updating dcs and topics from cassandra:", err)
			}
		}
	}()

	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, syscall.SIGTERM, syscall.SIGINT)

	<-stopChan
	fmt.Println("Got shutdown signal, gracefully shutting down")
	flushTicker.Stop()
	updateTicker.Stop()
	store.CloseSession()
	grpcS.GracefulStop()
	lis.Close()
}
