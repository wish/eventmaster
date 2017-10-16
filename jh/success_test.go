package jh

import (
	"net/http"
	"reflect"
	"testing"
)

func TestSuccess(t *testing.T) {
	data := map[string]int{
		"a": 1,
		"b": 2,
	}

	s := NewSuccess(data, http.StatusCreated)

	if got, want := s.Status(), http.StatusCreated; got != want {
		t.Fatalf("incorrect status; got: %v, want %v", got, want)
	}

	d, ok := s.Data().(map[string]int)
	if !ok {
		t.Fatal("unable to convert data from empty interface")
	}

	if !reflect.DeepEqual(d, data) {
		t.Fatalf("unequal maps")
	}
}
