package jh

import (
	"net/http"

	"github.com/pkg/errors"
)

// Error is an error interface that also tracks http status codes
type Error interface {
	error
	HasStatus
}

// NewError returns an Error with a given http status code
func NewError(msg string, status int) Error {
	return &e{
		msg:    msg,
		status: status,
	}
}

// HasStatus is an interface that allows a type to expose an http status code.
type HasStatus interface {
	Status() int
}

type e struct {
	msg    string
	status int
}

func (e *e) Status() int {
	return e.status
}

func (e *e) Error() string {
	return e.msg
}

// Wrap behaves as github.com/pkg/errors.Wrap, but preserves http status codes.
func Wrap(err error, msg string) Error {
	if err == nil {
		return nil
	}
	status := http.StatusInternalServerError
	switch err := err.(type) {
	case Error:
		status = err.Status()
	}
	return NewError(errors.Wrap(err, msg).Error(), status)
}
