package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"

	"google.golang.org/grpc"

	pb "github.com/ContextLogic/eventmaster/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

const usage = `emctl [(in)ject|(l)oad]`

func main() {
	host := "localhost:50052"
	if h := os.Getenv("EM_HOST"); h != "" {
		host = h
	}

	conc := 16
	if w := os.Getenv("EM_CONCURRENCY"); w != "" {
		i, err := strconv.Atoi(w)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parsing width: %v\n", err)
			os.Exit(1)
		}
		conc = i
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		s := <-sigs
		log.Printf("%v", s)
		cancel()
	}()

	conn, err := grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewEventMasterClient(conn)

	cmd := ""
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	switch cmd {
	case "in", "inject":
		if err := inject(ctx, c); err != nil {
			fmt.Fprintf(os.Stderr, "injection: %v\n", err)
			os.Exit(1)
		}
	case "l", "load":
		prometheus.MustRegister(counts, latency)
		http.Handle("/metrics", promhttp.Handler())
		go func() {
			log.Fatal(http.ListenAndServe(":8080", nil))
		}()
		if err := load(ctx, c, conc); err != nil {
			fmt.Fprintf(os.Stderr, "load: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "usage: %v\n", usage)
		os.Exit(1)
	}
}
