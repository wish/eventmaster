package main

import (
	"context"
	"fmt"
	"sort"

	pb "github.com/wish/eventmaster/proto"
	"github.com/pkg/errors"
)

func listDC(ctx context.Context, c pb.EventMasterClient) error {
	dcs, err := c.GetDCs(ctx, &pb.EmptyRequest{})
	if err != nil {
		return errors.Wrap(err, "getting dcs")
	}
	s := []string{}
	for _, dc := range dcs.Results {
		s = append(s, dc.DCName)
	}
	sort.Strings(s)
	for _, t := range s {
		fmt.Println(t)
	}
	return nil
}
