package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"google.golang.org/grpc"

	"github.com/wish/eventmaster"
	pb "github.com/wish/eventmaster/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

const usage = `emctl [(in)ject|(l)oad|(t)opic|dc]`
const topicUsage = `emctl topic [list]`
const dcUsage = `emctl dc [list]`

func main() {
	cfg, err := parseConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "parsing config: %v\n", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		s := <-sigs
		log.Printf("%v", s)
		cancel()
	}()

	conn, err := grpc.Dial(cfg.Host, grpc.WithInsecure())
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
	case "env":
		fmt.Printf(cfg.String())
		os.Exit(1)
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
		if err := load(ctx, c, cfg.Concurrency); err != nil {
			fmt.Fprintf(os.Stderr, "load: %v\n", err)
			os.Exit(1)
		}
	case "t", "topic":
		rest := os.Args[1:]
		sub := ""
		if len(rest) > 1 {
			sub = rest[1]
		}
		switch sub {
		case "ls", "list":
			if err := listTopic(ctx, c); err != nil {
				fmt.Fprintf(os.Stderr, "topic list: %v\n", err)
				os.Exit(1)
			}
		default:
			fmt.Fprintf(os.Stderr, "usage: %v\n", topicUsage)
			os.Exit(1)
		}
	case "dc":
		rest := os.Args[1:]
		sub := ""
		if len(rest) > 1 {
			sub = rest[1]
		}
		switch sub {
		case "ls", "list":
			if err := listDC(ctx, c); err != nil {
				fmt.Fprintf(os.Stderr, "topic list: %v\n", err)
				os.Exit(1)
			}
		default:
			fmt.Fprintf(os.Stderr, "usage: %v\n", dcUsage)
			os.Exit(1)
		}
	case "v", "version":
		eventmaster.PrintVersions()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "usage: %v\n", usage)
		os.Exit(1)
	}
}
