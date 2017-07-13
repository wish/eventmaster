package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
	metrics "github.com/rcrowley/go-metrics"
)

type rsyslogServer struct {
	lis   net.Listener
	store *EventStore
	timer metrics.Timer
}

func NewRsyslogServer(config *Config, s *EventStore, r metrics.Registry) (*rsyslogServer, error) {
	var lis net.Listener
	if config.CAFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "Error loading X509 Key pair from files")
		}

		caCert, err := ioutil.ReadFile(config.CAFile)
		if err != nil {
			return nil, errors.Wrap(err, "Error loading ca cert from file")
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}

		lis, err = tls.Listen("tcp", fmt.Sprintf(":%d", config.RsyslogPort), tlsConfig)
		if err != nil {
			return nil, errors.Wrap(err, "Error creating tls listener")
		}
	} else {
		var err error
		lis, err = net.Listen("tcp", fmt.Sprintf(":%d", config.RsyslogPort))
		if err != nil {
			return nil, errors.Wrap(err, "Error creating net listener")
		}
	}
	fmt.Println("Starting rsyslog server on port", config.RsyslogPort)

	return &rsyslogServer{
		lis:   lis,
		store: s,
		timer: metrics.GetOrRegisterTimer("rsyslog:Timer", r),
	}, nil
}

func (s *rsyslogServer) handleLogRequest(conn net.Conn) {
	start := time.Now()
	defer s.timer.UpdateSince(start)

	buf := make([]byte, 20000)
	_, err := conn.Read(buf)
	if err != nil {
		// TODO: keep metric on this
		fmt.Println("Error reading log:", err.Error())
		return
	}

	logs := strings.Split(string(buf), "\n")
	for _, log := range logs {
		parts := strings.Split(log, "^0")
		if len(parts) < 5 {
			continue
		}

		var timestamp int64
		timeObj, err := time.Parse(time.RFC3339, parts[0])
		if err != nil {
			fmt.Println("Error parsing timestamp, using current timestamp", err)
			timestamp = time.Now().Unix()
		} else {
			timestamp = timeObj.Unix()
		}
		dc, host, topic, message := parts[1], parts[2], parts[3], parts[4]
		data := parseKeyValuePair(message)
		user := ""
		if val, ok := data["uid"]; ok {
			user = val.(string)
		} else if val, ok := data["ouid"]; ok {
			user = val.(string)
		}

		var tags []string
		if val, ok := data["type"]; ok {
			tags = append(tags, val.(string))
		}

		_, err = s.store.AddEvent(&UnaddedEvent{
			EventTime: timestamp,
			TopicName: topic,
			Dc:        dc,
			Tags:      tags,
			Host:      host,
			User:      user,
			Data:      data,
		})
		if err != nil {
			// TODO: keep metric on this, add to queue of events to retry?
			fmt.Println("Error adding log event", err)
		}
	}
}

func (s *rsyslogServer) AcceptLogs() {
	go func() {
		for {
			conn, err := s.lis.Accept()
			if err != nil {
				// TODO: add stats on error
				fmt.Println("Error accepting logs:", err.Error())
			}

			go s.handleLogRequest(conn)
		}
	}()
}

func (s *rsyslogServer) Stop() error {
	return s.lis.Close()
}
