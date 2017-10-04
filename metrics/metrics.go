package metrics

import (
	"fmt"
	"time"
)

// EventStoreLatency records eventstore latency for a named op.
func EventStoreLatency(op string, start time.Time) {
	eventStoreTimer.WithLabelValues("Find").Observe(msSince(start))
}

// DBError increments a counter for a db error operation.
func DBError(op string) {
	eventStoreDbErrCounter.WithLabelValues(op).Inc()
}

// GPRC records grpc request latency for a named method.
func GRPCLatency(method string, start time.Time) {
	grpcReqLatencies.WithLabelValues(method).Observe(msSince(start))
}

// GRPCSuccess counts successful gPRC calls by method.
func GRPCSuccess(method string) {
	grpcRespCounter.WithLabelValues(method, "0").Inc()
}

// GRPCFailure counts failed gPRC calls by method.
func GRPCFailure(method string) {
	grpcRespCounter.WithLabelValues(method, "1").Inc()
}

// Rsyslog records rsyslog latency.
func RsyslogLatency(start time.Time) {
	rsyslogReqLatencies.WithLabelValues().Observe(msSince(start))
}

//
func HTTPLatency(path string, start time.Time) {
	httpReqLatencies.WithLabelValues(path).Observe(msSince(start))
}

func HTTPStatus(path string, status int) {
	httpStatus.WithLabelValues(path, fmt.Sprintf("%d", bucketHTTPStatus(status))).Inc()
}

// bucketHTTPStatus rounds down to the nearest hundred to facilitate categorizing http statuses.
func bucketHTTPStatus(i int) int {
	return i - i%100
}
