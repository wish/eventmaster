package eventmaster

import (
	"net/http"
)

// StatusRecorder is a simple http status recorder
type StatusRecorder struct {
	http.ResponseWriter

	status int
}

// NewStatusRecorder returns an initialized StatusRecorder, with 200 as the
// default status.
func NewStatusRecorder(w http.ResponseWriter) *StatusRecorder {
	return &StatusRecorder{ResponseWriter: w, status: http.StatusOK}
}

// Status returns the cached http status value.
func (sr *StatusRecorder) Status() int {
	return sr.status
}

// WriteHeader caches the status, then calls the underlying ResponseWriter.
func (sr *StatusRecorder) WriteHeader(status int) {
	sr.status = status
	sr.ResponseWriter.WriteHeader(status)
}
