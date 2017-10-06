package main

import (
	"context"
	"log"
	"os/user"

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

	for _, dc := range dcs.Results {
		for _, topic := range topics.Results {
			e := &pb.Event{
				DC:        dc.DCName,
				TopicName: topic.TopicName,
				Host:      "inject.emctl.i.wish.com",
				TagSet:    []string{"a", "b"},
				User:      u.Username,
			}
			resp, err := c.AddEvent(ctx, e)
			if err != nil {
				return errors.Wrap(err, "add event")
			}
			log.Printf("%v %v %v", resp.Id, dc.DCName, topic.TopicName)
		}
	}
	return nil
}
