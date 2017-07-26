package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type rsyslogServer struct {
	lis   net.Listener
	store *EventStore
}

func NewRsyslogServer(s *EventStore, tlsConfig *tls.Config, port int) (*rsyslogServer, error) {
	var lis net.Listener
	var err error
	if tlsConfig != nil {
		lis, err = tls.Listen("tcp", fmt.Sprintf(":%d", port), tlsConfig)
	} else {
		lis, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
	}
	if err != nil {
		return nil, errors.Wrap(err, "Error creating net listener")
	}
	fmt.Println("Starting rsyslog server on port", port)

	return &rsyslogServer{
		lis:   lis,
		store: s,
	}, nil
}

func (s *rsyslogServer) handleLogRequest(conn net.Conn) {
	start := time.Now()
	defer func() {
		rsyslogReqLatencies.WithLabelValues().Observe(trackTime(start))
	}()
	rsyslogReqCounter.WithLabelValues().Inc()

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
