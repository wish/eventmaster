package main

import (
	"context"
	"fmt"
	"sort"

	pb "github.com/ContextLogic/eventmaster/proto"
	"github.com/pkg/errors"
)

func listTopic(ctx context.Context, c pb.EventMasterClient) error {
	topics, err := c.GetTopics(ctx, &pb.EmptyRequest{})
	if err != nil {
		return errors.Wrap(err, "getting topics")
	}
	ts := []string{}
	for _, topic := range topics.Results {
		ts = append(ts, topic.TopicName)
	}
	sort.Strings(ts)
	for _, t := range ts {
		fmt.Println(t)
	}
	return nil
}
