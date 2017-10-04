package main

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
)

type config struct {
	Host        string
	Concurrency int
}

func parseConfig() (config, error) {
	c := config{
		Host:        "localhost:50052",
		Concurrency: 1,
	}
	if err := envconfig.Process("em", &c); err != nil {
		return c, errors.Wrap(err, "parsing environment")
	}
	return c, nil
}

func (c config) String() string {
	var r string
	r += fmt.Sprintf("EM_HOST=%v\n", c.Host)
	r += fmt.Sprintf("EM_CONCURRENCY=%v\n", c.Concurrency)
	return r
}
