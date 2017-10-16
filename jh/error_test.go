package jh

import (
	"net/http"
	"testing"

	"github.com/pkg/errors"
)

func TestNewHTTPError(t *testing.T) {
	e := NewError("foo", http.StatusTeapot)

	switch e.(type) {
	case Error:
	default:
		t.Fatal("NewError did not return an Error")
	}

	ee := NewError(errors.Wrap(e, "more context!").Error(), http.StatusBadRequest)

	switch ee.(type) {
	case Error:
	default:
		t.Fatalf("NewError with a wrapped error did not return an Error")
	}
}

func TestWrap(t *testing.T) {
	{
		var err error
		if err := Wrap(err, "nothing"); err != nil {
			t.Fatalf("Wrap did not leave nil error alone: %v", err)
		}
	}

	{
		err := Wrap(errors.New("foo"), "more")
		if got, want := err.Status(), http.StatusInternalServerError; got != want {
			t.Fatalf("wrong status; got: %v, want: %v", got, want)
		}
	}

	{
		s := http.StatusTeapot
		err := Wrap(NewError("foo", s), "more")
		if got, want := err.Status(), http.StatusTeapot; got != want {
			t.Fatalf("wrong status; got: %v, want: %v", got, want)
		}
	}
}
