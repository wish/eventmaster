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

	eventmaster "github.com/ContextLogic/eventmaster/proto"
	log "github.com/Sirupsen/logrus"
	"github.com/jessevdk/go-flags"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type emConfig struct {
	DataStore      string          `json:"data_store"`
	CassConfig     CassandraConfig `json:"cassandra_config"`
	UpdateInterval int             `json:"update_interval"`
}

func getEmConfig() emConfig {
	emConf := emConfig{}
	confFile, err := ioutil.ReadFile("em_config.json")
	if err != nil {
		fmt.Println("No em_config file specified")
	} else {
		err = json.Unmarshal(confFile, &emConf)
		if err != nil {
			fmt.Println("Error parsing em_config.json, using defaults:", err)
		}
	}
	if emConf.UpdateInterval == 0 {
		emConf.UpdateInterval = 5
	}
	return emConf
}

func setupPlugins() {
	if _, err := os.Stat("plugins/github.json"); err == nil {
		fmt.Println("Setting up GitHub plugin")
		NewGitHubPlugin()
	}
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
	emConf := getEmConfig()
	var ds DataStore
	var err error
	if emConf.DataStore == "cassandra" {
		ds, err = NewCassandraStore(emConf.CassConfig)
		if err != nil {
			log.Fatalf("failed to create cassandra data store: %v", err)
		}
	} else {
		log.Fatalf("Unrecognized data store option")
	}
	store, err := NewEventStore(ds)
	if err != nil {
		log.Fatalf("Unable to create event store: %v", err)
	}
	if err := store.Update(); err != nil {
		fmt.Println("Error loading dcs and topics from Cassandra", err)
	}

	setupPlugins()

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
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    caCertPool,
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
	reflection.Register(grpcS)

	go httpS.Serve(httpL)
	go grpcS.Serve(grpcL)

	go func() {
		fmt.Println("Starting server on port", config.Port)
		if err := mux.Serve(); err != nil {
			log.Fatalf("Error starting server: %v", err)
		}
	}()

	updateTicker := time.NewTicker(time.Second * time.Duration(emConf.UpdateInterval))
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
	fmt.Println("Got shutdown signal, gracefully shutting down...")
	updateTicker.Stop()
	store.CloseSession()
	grpcS.GracefulStop()
	lis.Close()
	if config.RsyslogServer {
		rsyslogServer.Stop()
	}
}
