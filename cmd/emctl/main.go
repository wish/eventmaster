package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"google.golang.org/grpc"

	pb "github.com/ContextLogic/eventmaster/proto"
)

const usage = `emctl [(in)ject|(l)oad]`

func main() {
	host := "localhost:50052"
	if h := os.Getenv("EM_HOST"); h != "" {
		host = h
	}

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
		if err := inject(context.Background(), c); err != nil {
			fmt.Fprintf(os.Stderr, "injection: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "usage: %v\n", usage)
		os.Exit(1)
	}
}
