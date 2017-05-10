package main

import (
    "encoding/json"
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
    "github.com/gocql/gocql"
    "github.com/jessevdk/go-flags"
    metrics "github.com/rcrowley/go-metrics"
    "github.com/rcrowley/go-metrics/exp"
    "google.golang.org/grpc"
    "google.golang.org/grpc/reflection"
)

type dbConfig struct {
    host string
    port string
    keyspace string
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

    // Establish connection to Cassandra
    dbConf := dbConfig{}
    confFile, err := os.Open("db_config.json")
    if err != nil {
        decoder := json.NewDecoder(confFile)
        err = decoder.Decode(&dbConf)
        if err != nil {
            fmt.Println("error parsing db_config.json:", err)
        }
    }
    if dbConf.host == "" {
        dbConf.host = "127.0.0.1"
    }
    if dbConf.port == "" {
        dbConf.port = "9042"
    }
    if dbConf.keyspace == "" {
        dbConf.keyspace = "event_master"
    }
    cluster := gocql.NewCluster(fmt.Sprintf("%s:%s", dbConf.host, dbConf.port))
    cluster.Keyspace = dbConf.keyspace
    cluster.Consistency = gocql.Quorum
    session, err := cluster.CreateSession()
    if err != nil {
        log.Fatalf("Error connecting to Cassandra: %v", err)
    }

    // Create the EventMaster server
    server, err := NewServer(&config, session)
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
    <-stopChan
    fmt.Println("Got shutdown signal, gracefully shutting down")
    session.Close()
    s.GracefulStop()
}
