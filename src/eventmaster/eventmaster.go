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
	"github.com/rcrowley/go-metrics/exp"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

type dbConfig struct {
	CassandraAddr  []string `json:"cassandra_addr"`
	Keyspace       string   `json:"cassandra_keyspace"`
	Consistency    string   `json:"consistency"`
	ESAddr         []string `json:"es_addr"`
	ESUsername     string   `json:"es_username"`
	ESPassword     string   `json:"es_password"`
	FlushInterval  int      `json:"flush_interval"`
	UpdateInterval int      `json:"update_interval"`
	CertFile       string   `json:"cert_file"`
	KeyFile        string   `json:"key_file"`
	CAFile         string   `json:"ca_file"`
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
	if dbConf.Keyspace == "" {
		dbConf.Keyspace = "event_master"
	}
	if dbConf.Consistency == "" {
		dbConf.Consistency = "local_quorum"
	}
	if dbConf.FlushInterval == 0 {
		dbConf.FlushInterval = 5
	}
	if dbConf.UpdateInterval == 0 {
		dbConf.UpdateInterval = 5
	}
	if dbConf.CassandraAddr == nil || len(dbConf.CassandraAddr) == 0 {
		dbConf.CassandraAddr = append(dbConf.CassandraAddr, "127.0.0.1:9042")
	}
	if dbConf.ESAddr == nil || len(dbConf.ESAddr) == 0 {
		dbConf.ESAddr = append(dbConf.ESAddr, "http://127.0.0.1:9200")
	}
	return dbConf
}

func parseKeyValuePair(content string) map[string]interface{} {
	data := make(map[string]interface{})
	pairs := strings.Split(content, " ")
	for _, pair := range pairs {
		parts := strings.Split(pair, "=")
		data[parts[0]] = parts[1]
	}
	return data
}

func handleLogRequest(conn net.Conn, store *EventStore) {
	buf := make([]byte, 1024)
	_, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading log:", err.Error())
	}
	fmt.Println(string(buf))
	parts := strings.Split(string(buf), "|")
	if len(parts) < 5 {
		fmt.Println("Log message is not in correct format")
		return
	}

	var timestamp int64
	timeObj, err := time.Parse(time.RFC3339, parts[0])
	if err != nil {
		fmt.Println("Error parsing timestamp", err)
		timestamp = time.Now().Unix()
	} else {
		timestamp = timeObj.Unix()
	}
	dc := parts[1]
	host := parts[2]
	topic := parts[3]
	message := parts[4]
	data := parseKeyValuePair(message)
	user := ""
	if val, ok := data["uid"]; ok {
		user = val.(string)
	} else if val, ok := data["ouid"]; ok {
		user = val.(string)
	}

	var tags []string
	if val, ok := data["type"]; ok {
		tags = append(tags, val.(string))
	}

	_, err = store.AddEvent(&UnaddedEvent{
		EventTime: timestamp,
		TopicName: topic,
		Dc:        dc,
		Tags:      tags,
		Host:      host,
		User:      user,
		Data:      data,
	})
	if err != nil {
		fmt.Println("Error adding log event", err)
	}
}

func getHTTPServer(store *EventStore, registry metrics.Registry) *http.Server {
	r := httprouter.New()
	h := httpHandler{
		store: store,
	}

	// API endpoints
	r.POST("/v1/event", wrapHandler(h.handleAddEvent, registry))
	r.GET("/v1/event", wrapHandler(h.handleGetEvent, registry))
	r.POST("/v1/topic", wrapHandler(h.handleAddTopic, registry))
	r.PUT("/v1/topic/:name", wrapHandler(h.handleUpdateTopic, registry))
	r.GET("/v1/topic", wrapHandler(h.handleGetTopic, registry))
	r.DELETE("/v1/topic/:name", wrapHandler(h.handleDeleteTopic, registry))
	r.POST("/v1/dc", wrapHandler(h.handleAddDc, registry))
	r.PUT("/v1/dc/:name", wrapHandler(h.handleUpdateDc, registry))
	r.GET("/v1/dc/", wrapHandler(h.handleGetDc, registry))

	// GitHub webhook endpoint
	r.POST("/v1/github_event", wrapHandler(h.handleGitHubEvent, registry))

	// UI endpoints
	r.GET("/", HandleMainPage)
	r.GET("/add_event", HandleCreatePage)
	r.GET("/topic", HandleTopicPage)
	r.GET("/dc", HandleDcPage)

	// JS file endpoints
	r.ServeFiles("/js/*filepath", http.Dir("ui/js"))
	r.ServeFiles("/bootstrap/*filepath", http.Dir("ui/bootstrap"))
	r.ServeFiles("/css/*filepath", http.Dir("ui/css"))

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
	store, err := NewEventStore(dbConf, config, r)
	if err != nil {
		log.Fatalf("Unable to create event store: %v", err)
	}
	if err := store.Update(); err != nil {
		fmt.Println("Error loading dcs and topics from Cassandra", err)
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

	logLis, err := net.Listen("tcp", "0.0.0.0:8044")
	if err != nil {
		log.Fatalf("Error starting tcp server: %v", err)
	}
	fmt.Println("Starting TCP server for logs on port 8044")

	go func() {
		for {
			conn, err := logLis.Accept()
			if err != nil {
				fmt.Println("Error accepting logs:", err.Error())
			}

			go handleLogRequest(conn, store)
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
	logLis.Close()
}
