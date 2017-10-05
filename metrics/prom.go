package metrics

import (
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	httpReqLatencies = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "eventmaster",
		Subsystem: "http_server",
		Name:      "request_latency_ms",
		Help:      "Latency in ms of http requests grouped by req path",
		Buckets:   buckets(),
	}, []string{"path"})

	httpStatus = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "eventmaster",
		Subsystem: "http_server",
		Name:      "status_count",
		Help:      "The count of http responses issued classified by status and api endpoint",
	}, []string{"path", "code"})

	grpcReqLatencies = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "eventmaster",
		Subsystem: "grpc_server",
		Name:      "request_latency",
		Help:      "Latency of grpc requests grouped by method name",
		Buckets:   buckets(),
	}, []string{"method"})

	grpcRespCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "eventmaster",
		Subsystem: "grpc_server",
		Name:      "response_count",
		Help:      "The count of grpc responses issued classified by method name and success",
	}, []string{"method", "success"})

	rsyslogReqLatencies = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "eventmaster",
		Subsystem: "rsyslog_server",
		Name:      "request_latency",
		Help:      "Latency of rsyslog requests",
		Buckets:   buckets(),
	}, []string{})

	eventStoreTimer = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "eventmaster",
		Subsystem: "event_store",
		Name:      "method_time",
		Help:      "Time of event store methods by method name",
		Buckets:   buckets(),
	}, []string{"method"})

	eventStoreDbErrCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "eventmaster",
		Subsystem: "event_store",
		Name:      "db_error",
		Help:      "The count of db errors by db name and type of operation",
	}, []string{"operation"})
)

// RegisterPromMetrics registers all the metrics that eventmanger uses.
func RegisterPromMetrics() error {
	if err := prometheus.Register(httpReqLatencies); err != nil {
		return errors.Wrap(err, "registering http request latency")
	}

	if err := prometheus.Register(httpStatus); err != nil {
		return errors.Wrap(err, "registering http response counter")
	}

	if err := prometheus.Register(grpcReqLatencies); err != nil {
		return errors.Wrap(err, "registering gRPC request latency")
	}

	if err := prometheus.Register(grpcRespCounter); err != nil {
		return errors.Wrap(err, "registering gRPC response counter")
	}

	if err := prometheus.Register(rsyslogReqLatencies); err != nil {
		return errors.Wrap(err, "registering rsyslog request latency")
	}

	if err := prometheus.Register(eventStoreTimer); err != nil {
		return errors.Wrap(err, "registering eventstore timer")
	}

	if err := prometheus.Register(eventStoreDbErrCounter); err != nil {
		return errors.Wrap(err, "registering event store errors")
	}

	return nil
}

// msSince returns milliseconds since start.
func msSince(start time.Time) float64 {
	return float64(time.Since(start) / time.Millisecond)
}

// buckets returns the default prometheus buckets scaled to milliseconds.
func buckets() []float64 {
	r := []float64{}

	for _, v := range prometheus.DefBuckets {
		r = append(r, v*float64(time.Second/time.Millisecond))
	}
	return r
}
