package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
	log "github.com/Sirupsen/logrus"
	"github.com/jessevdk/go-flags"
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
}

func getDbConfig() dbConfig {
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

func main() {
	var config Config
	parser := flags.NewParser(&config, flags.Default)
	if _, err := parser.Parse(); err != nil {
		log.Fatalf("Error parsing flags: %v", err)
	}

	if config.PromExporter {
		err := registerPromMetrics()
		if err != nil {
			log.Fatalf("Unable to register prometheus metrics: %v", err)
		}

		promL, err := net.Listen("tcp", fmt.Sprintf(":%d", config.PromPort))
		if err != nil {
			log.Fatalf("failed to start prom client: %v", err)
		}
		fmt.Println("starting prometheus exporter on port", config.PromPort)
		go http.Serve(promL, GetPromHandler())
	}

	// Set up event store
	dbConf := getDbConfig()
	store, err := NewEventStore(dbConf, config)
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

	var tlsConfig *tls.Config
	if config.CAFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			log.Fatalf("Failed to load X509 key pair %v", err)
		}

		caCert, err := ioutil.ReadFile(config.CAFile)
		if err != nil {
			log.Fatalf("Failed to load CA cert file %v", err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
	}

	httpS := NewHTTPServer(tlsConfig, store)

	// Create the EventMaster grpc server
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

	rsyslogServer := &rsyslogServer{}

	if config.RsyslogServer {
		rsyslogServer, err = NewRsyslogServer(store, tlsConfig, config.RsyslogPort)
		if err != nil {
			log.Fatalf("Unable to start server: %v", err)
		}
		rsyslogServer.AcceptLogs()
	}

	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, syscall.SIGTERM, syscall.SIGINT)

	<-stopChan
	fmt.Println("Got shutdown signal, gracefully shutting down")
	flushTicker.Stop()
	updateTicker.Stop()
	store.CloseSession()
	grpcS.GracefulStop()
	lis.Close()
	if config.RsyslogServer {
		rsyslogServer.Stop()
	}
}
