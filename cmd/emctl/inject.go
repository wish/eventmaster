package main

import (
	"context"
	"log"
	"os/user"
	"sort"
	"time"

	"github.com/pkg/errors"

	pb "github.com/ContextLogic/eventmaster/proto"
)

func inject(ctx context.Context, c pb.EventMasterClient) error {
	u, err := user.Current()
	if err != nil {
		return errors.Wrap(err, "getting user")
	}
	dcs, err := c.GetDCs(ctx, &pb.EmptyRequest{})
	if err != nil {
		return errors.Wrap(err, "getting dcs")
	}
	topics, err := c.GetTopics(ctx, &pb.EmptyRequest{})
	if err != nil {
		return errors.Wrap(err, "getting topics")
	}

	ds, ts := []string{}, []string{}
	for _, dc := range dcs.Results {
		ds = append(ds, dc.DCName)
	}
	for _, tc := range topics.Results {
		ts = append(ts, tc.TopicName)
	}

	sort.Strings(ds)
	sort.Strings(ts)

	for _, dc := range ds {
		for _, topic := range ts {
			e := &pb.Event{
				DC:        dc,
				TopicName: topic,
				Host:      "inject.emctl.i.wish.com",
				TagSet:    []string{"a", "b"},
				User:      u.Username,
			}
			resp, err := c.AddEvent(ctx, e)
			if err != nil {
				return errors.Wrap(err, "add event")
			}
			log.Printf("%v %v %v", resp.ID, dc, topic)
			time.Sleep(2 * time.Second)
		}
	}
	return nil
}
