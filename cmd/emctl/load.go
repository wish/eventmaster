package main

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/wish/eventmaster/proto"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

func load(ctx context.Context, c pb.EventMasterClient, conc int) error {
	dcs, err := c.GetDCs(ctx, &pb.EmptyRequest{})
	if err != nil {
		return errors.Wrap(err, "getting dcs")
	}
	topics, err := c.GetTopics(ctx, &pb.EmptyRequest{})
	if err != nil {
		return errors.Wrap(err, "getting topics")
	}

	if len(dcs.Results) == 0 {
		return errors.New("no DC found")
	}
	if len(topics.Results) == 0 {
		return errors.New("no topics found")
	}
	dc, topic := dcs.Results[0], topics.Results[0]

	sub := func(hosts <-chan string) error {
		for host := range hosts {
			e := &pb.Event{
				DC:        dc.DCName,
				TopicName: topic.TopicName,
				Host:      host,
				TagSet:    []string{"a", "b"},
			}
			b := time.Now()
			_, err := c.AddEvent(ctx, e)
			if err != nil {
				return errors.Wrap(err, "add event")
			}
			latency.Observe(float64(time.Since(b) / time.Millisecond))
			counts.Inc()
		}
		return nil
	}

	wg := sync.WaitGroup{}
	hosts := gen(ctx)
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			err := sub(hosts)
			if err != nil {
				log.Printf("%v", err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}

func gen(ctx context.Context) <-chan string {
	r := make(chan string)
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Printf("done")
				close(r)
				return
			default:
				r <- uuid.NewV4().String()
			}
		}
	}()
	return r
}
