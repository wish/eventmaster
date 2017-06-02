package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
	log "github.com/Sirupsen/logrus"
	"github.com/jessevdk/go-flags"
	"github.com/julienschmidt/httprouter"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

type dbConfig struct {
	Host           string `json:"host"`
	Port           string `json:"port"`
	Keyspace       string `json:"keyspace"`
	Consistency    string `json:"consistency"`
	ESUrl          string `json:"es_url"`
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
	if dbConf.Host == "" {
		dbConf.Host = "127.0.0.1"
	}
	if dbConf.Port == "" {
		dbConf.Port = "9042"
	}
	if dbConf.Keyspace == "" {
		dbConf.Keyspace = "event_master"
	}
	if dbConf.Consistency == "" {
		dbConf.Consistency = "quorum"
	}
	if dbConf.ESUrl == "" {
		dbConf.ESUrl = "http://127.0.0.1:9200"
	}
	if dbConf.FlushInterval == 0 {
		dbConf.FlushInterval = 5
	}
	if dbConf.UpdateInterval == 0 {
		dbConf.UpdateInterval = 5
	}
	return dbConf
}

func getHTTPServer(store *EventStore) *http.Server {
	r := httprouter.New()
	aeah := &addEventAPIHandler{
		store: store,
	}
	geah := &getEventAPIHandler{
		store: store,
	}
	atah := &addTopicAPIHandler{
		store: store,
	}
	utah := &updateTopicAPIHandler{
		store: store,
	}
	gtah := &getTopicAPIHandler{
		store: store,
	}
	adah := &addDcAPIHandler{
		store: store,
	}
	udah := &updateDcAPIHandler{
		store: store,
	}
	gdah := &getDcAPIHandler{
		store: store,
	}
	r.Handler("POST", "/v1/event", aeah)
	r.Handler("GET", "/v1/event", geah)
	r.Handler("POST", "/v1/topic", atah)
	r.Handler("PUT", "/v1/topic/:name", utah)
	r.Handler("GET", "/v1/topic", gtah)
	r.Handler("POST", "/v1/dc", adah)
	r.Handler("PUT", "/v1/dc/:name", udah)
	r.Handler("GET", "/v1/dc", gdah)

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
	r.Handler("GET", "/", mph)
	r.Handler("GET", "/add_event", cph)
	r.Handler("GET", "/topic", tph)
	r.Handler("GET", "/dc", dph)

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

	exp.Exp(metrics.DefaultRegistry)
	sock, err := net.Listen("tcp", "0.0.0.0:12345")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	go func() {
		fmt.Println("go-metrics server listening at port 12345")
		http.Serve(sock, nil)
	}()

	g := metrics.NewGauge()
	metrics.Register("goroutines", g)
	go func() {
		for {
			g.Update(int64(runtime.NumGoroutine()))
			time.Sleep(time.Duration(10) * time.Second)
		}
	}()

	// Set up event store
	dbConf := getConfig()
	store, err := NewEventStore(dbConf)
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

	httpS := getHTTPServer(store)

	// Create the EventMaster server
	grpcServer, err := NewGRPCServer(&config, store)
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
		for _ = range flushTicker.C {
			if err := store.FlushToES(); err != nil {
				fmt.Println("Error flushing events from temp_event to ES:", err)
			}
		}
	}()

	updateTicker := time.NewTicker(time.Second * time.Duration(dbConf.UpdateInterval))
	go func() {
		for _ = range updateTicker.C {
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
