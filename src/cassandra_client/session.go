package cassandra_client

import (
	"github.com/gocql/gocql"
)

type Session interface {
	ExecQuery(string) error
	ExecIterQuery(query string) (ScanIter, CloseIter)
	Close()
}

type CqlSession struct {
	session *gocql.Session
}

type ScanIter func(...interface{}) bool
type CloseIter func() error

func NewCqlSession(ips []string, keyspace string, consistency string) (*CqlSession, error) {
	cluster := gocql.NewCluster(ips...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.ParseConsistency(consistency)

	s, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &CqlSession{
		session: s,
	}, nil
}

func (s *CqlSession) ExecQuery(query string) error {
	return s.session.Query(query).Exec()
}

func (s *CqlSession) ExecIterQuery(query string) (ScanIter, CloseIter) {
	iter := s.session.Query(query).Iter()
	return func(dest ...interface{}) bool {
			return iter.Scan(dest...)
		}, func() error {
			return iter.Close()
		}
}

func (s *CqlSession) Close() {
	s.session.Close()
}

type MockCassSession struct {
	query string
}

func (s *MockCassSession) ExecQuery(query string) error {
	s.query = query
	return nil
}

func (s *MockCassSession) ExecIterQuery(query string) (ScanIter, CloseIter) {
	s.query = query
	return func(dest ...interface{}) bool {
			return false
		},
		func() error {
			return nil
		}
}

func (s *MockCassSession) Close() {}

func (s *MockCassSession) LastQuery() string {
	return s.query
}
