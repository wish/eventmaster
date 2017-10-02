package main

import "github.com/prometheus/client_golang/prometheus"

var (
	counts = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "inserts",
			Help: "tracks number of inserted events.",
		},
	)

	latency = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Subsystem:  "emctl",
			Name:       "req_ms",
			Help:       "request latency (ms)",
			Objectives: map[float64]float64{0.5: 0.05, 0.95: 0.001, 0.99: 0.001, 1.0: 0.0001},
		},
	)
)
