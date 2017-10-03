package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	flags "github.com/jessevdk/go-flags"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	em "github.com/ContextLogic/eventmaster"
	emproto "github.com/ContextLogic/eventmaster/proto"
)

func main() {
	var config em.Flags
	parser := flags.NewParser(&config, flags.Default)
	a, err := parser.Parse()
	if err != nil {
		os.Exit(1)
	}

	if len(a) > 0 {
		switch a[0] {
		case "v", "version":
			em.PrintVersions()
			os.Exit(0)
		}
	}

	if err := em.RegisterPromMetrics(); err != nil {
		log.Fatalf("Unable to register prometheus metrics: %v", err)
	}

	// Set up event store
	emConf, err := ParseEMConfig(config.ConfigFile)
	if err != nil {
		log.Fatalf("problem parsing config file: %v", err)
	}

	var ds em.DataStore
	if emConf.DataStore == "cassandra" {
		ds, err = em.NewCassandraStore(emConf.CassConfig)
		if err != nil {
			log.Fatalf("failed to create cassandra data store: %v", err)
		}
	} else {
		log.Fatalf("Unrecognized data store option")
	}
	store, err := em.NewEventStore(ds)
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
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    caCertPool,
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
	}

	httpS := &http.Server{
		Handler:   em.NewServer(store, config.StaticFiles, config.Templates),
		TLSConfig: tlsConfig,
	}

	grpcServer := em.NewGRPCServer(&config, store)

	maxMsgSizeOpt := grpc.MaxMsgSize(1024 * 1024 * 100)
	// Create the gRPC server and register our service
	grpcS := grpc.NewServer(maxMsgSizeOpt)
	emproto.RegisterEventMasterServer(grpcS, grpcServer)
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
	rsyslogServer := &em.RsyslogServer{}

	if config.RsyslogServer {
		rsyslogServer, err = em.NewRsyslogServer(store, tlsConfig, config.RsyslogPort)
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
