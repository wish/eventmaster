package eventmaster

import (
	"net/http"
)

// StatusRecorder is a simple http status recorder
type StatusRecorder struct {
	http.ResponseWriter

	status int
}

func NewStatusRecorder(w http.ResponseWriter) *StatusRecorder {
	return &StatusRecorder{ResponseWriter: w, status: http.StatusOK}
}

func (sr *StatusRecorder) Status() int {
	return sr.status
}

func (sw *StatusRecorder) WriteHeader(status int) {
	sw.status = status
	sw.ResponseWriter.WriteHeader(status)
}
