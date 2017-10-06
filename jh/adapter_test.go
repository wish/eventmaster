package jh

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/pkg/errors"
)

func TestAdapter(t *testing.T) {
	ts := httptest.NewServer(newServer())
	c := ts.Client()

	tests := []struct {
		label string
		route string
		code  int
		chk   func(io.Reader) error
	}{
		{
			label: "nominal",
			route: "/ok/",
			code:  http.StatusOK,
			chk: func(body io.Reader) error {
				j := map[string]int{}
				if err := json.NewDecoder(body).Decode(&j); err != nil {
					errors.Wrap(err, "json decode")
				}
				if got, want := j, resp(); !reflect.DeepEqual(got, want) {
					return fmt.Errorf("%v != %v", got, want)
				}
				return nil
			},
		},
		{
			label: "created",
			route: "/created/",
			code:  http.StatusCreated,
			chk: func(body io.Reader) error {
				j := map[string]int{}
				if err := json.NewDecoder(body).Decode(&j); err != nil {
					errors.Wrap(err, "json decode")
				}
				if got, want := j, resp(); !reflect.DeepEqual(got, want) {
					return fmt.Errorf("%v != %v", got, want)
				}
				return nil
			},
		},
		{
			label: "conflict",
			route: "/conflict/",
			code:  http.StatusConflict,
			chk: func(body io.Reader) error {
				j := map[string]string{}
				if err := json.NewDecoder(body).Decode(&j); err != nil {
					errors.Wrap(err, "json decode")
				}
				if got, want := j["error"], "context: reason"; got != want {
					return fmt.Errorf("got %q, want %q", got, want)
				}
				return nil
			},
		},
		{
			label: "default error",
			route: "/ise/",
			code:  http.StatusInternalServerError,
			chk: func(body io.Reader) error {
				j := map[string]string{}
				if err := json.NewDecoder(body).Decode(&j); err != nil {
					errors.Wrap(err, "json decode")
				}
				if got, want := j["error"], "should get default code"; got != want {
					return fmt.Errorf("got %q, want %q", got, want)
				}
				return nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			resp, err := c.Get(ts.URL + test.route)
			if err != nil {
				t.Fatalf("get: %v", err)
			}

			if got, want := resp.StatusCode, test.code; got != want {
				t.Fatalf("unexpected status code; got %v, want %v", got, want)
			}

			if test.chk != nil {
				if err := test.chk(resp.Body); err != nil {
					t.Fatalf("resp check: %v", err)
				}
			}
		})
	}
}
