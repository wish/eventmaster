package jh

// Success houses an http status code and data that will be encoded as JSON
// from the successful return from a JSONHandler.
type Success interface {
	HasStatus
	Data() interface{}
}

// NewSuccess exists to give the return from a JSONHandler an alternate http status.
//
// For example, if you need to communicate that a resource was created:
//
//	r := NewResource()
//	return NewSuccess(r, http.StatusCreated), nil
func NewSuccess(data interface{}, status int) Success {
	return &s{
		data:   data,
		status: status,
	}
}

type s struct {
	data   interface{}
	status int
}

func (s *s) Status() int {
	return s.status
}

func (s *s) Data() interface{} {
	return s.data
}
