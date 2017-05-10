package main

import (
    "github.com/gocql/gocql"
)

type EventStore struct {
    session *gocql.Session
}

func NewEventStore(s *gocql.Session) (*EventStore) {
    return &EventStore{
        session: s,
    }
}
