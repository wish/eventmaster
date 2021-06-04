package cassandra

import (
	"time"

	"github.com/gocql/gocql"
)

// Session is an interface that describes the surface area of interacting with
// a cassandra store.
type Session interface {
	ExecQuery(string) error
	ExecIterQuery(query string) (ScanIter, CloseIter)
	Close()
}

// CQLSession implements a session to cassandra.
type CQLSession struct {
	session *gocql.Session
}

// ScanIter defines the function type returned from ExecIterQuery.
type ScanIter func(...interface{}) bool

// CloseIter is a type returned from ExecIterQuery. When used it wraps the
// close of the underlying iterator.
type CloseIter func() error

func NewCQLConfig(ips []string, keyspace string, consistency string, timeout string) (*gocql.ClusterConfig, error) {
	cluster := gocql.NewCluster(ips...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.ParseConsistency(consistency)
	var err error
	cluster.Timeout, err = time.ParseDuration(timeout)
	if err != nil {
		return nil, err
	}
	return cluster, nil
}

func NewCQLSessionFromConfig(cluster *gocql.ClusterConfig) (*CQLSession, error) {
	s, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &CQLSession{
		session: s,
	}, nil
}

// NewCQLSession returns a populated CQLSession struct, or an error using the
// underlying cassandra driver.
func NewCQLSession(ips []string, keyspace string, consistency string, timeout string) (*CQLSession, error) {
	cluster, err := NewCQLConfig(ips, keyspace, consistency, timeout)
	if err != nil {
		return nil, err
	}

	return NewCQLSessionFromConfig(cluster)
}

// ExecQuery executes the provided query against the underlying cassandra
// session.
func (s *CQLSession) ExecQuery(query string) error {
	return s.session.Query(query).Exec()
}

// ExecIterQuery performs an iterated query against the underlying session.
func (s *CQLSession) ExecIterQuery(query string) (ScanIter, CloseIter) {
	iter := s.session.Query(query).Iter()

	return func(dest ...interface{}) bool {
			return iter.Scan(dest...)
		}, func() error {
			return iter.Close()
		}
}

// Close closes the underlying cassandra session.
func (s *CQLSession) Close() {
	s.session.Close()
}

// MockCassSession is used in testing.
type MockCassSession struct {
	query string
}

// ExecQuery implements the interface for testing.
func (s *MockCassSession) ExecQuery(query string) error {
	s.query = query
	return nil
}

// ExecIterQuery implements the interface for testing.
func (s *MockCassSession) ExecIterQuery(query string) (ScanIter, CloseIter) {
	s.query = query
	return func(dest ...interface{}) bool {
			return false
		},
		func() error {
			return nil
		}
}

// Close implements the interface for testing.
func (s *MockCassSession) Close() {}

// LastQuery is used during tests to validate the generated query.
func (s *MockCassSession) LastQuery() string {
	return s.query
}
