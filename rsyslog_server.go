package eventmaster

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type RsyslogServer struct {
	lis   net.Listener
	store *EventStore
}

type LogParser func(int64, string, string, string, string) *UnaddedEvent

var logParserMap = map[string]LogParser{
	"auditd": parseAuditd,
}

func parseAuditd(timestamp int64, dc string, host string, topic string, msg string) *UnaddedEvent {
	data := parseKeyValuePair(msg)
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

	return &UnaddedEvent{
		EventTime: timestamp,
		TopicName: topic,
		Dc:        dc,
		Tags:      tags,
		Host:      host,
		User:      user,
		Data:      data,
	}
}

func NewRsyslogServer(s *EventStore, tlsConfig *tls.Config, port int) (*RsyslogServer, error) {
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

	return &RsyslogServer{
		lis:   lis,
		store: s,
	}, nil
}

func (s *RsyslogServer) handleLogRequest(conn net.Conn) {
	start := time.Now()
	defer func() {
		rsyslogReqLatencies.WithLabelValues().Observe(trackTime(start))
	}()
	rsyslogReqCounter.WithLabelValues().Inc()

	buf := make([]byte, 20000)
	_, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading log:", err.Error())
	}
	defer conn.Close()

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
		if parser, ok := logParserMap[topic]; ok {
			evt := parser(timestamp, dc, host, topic, message)
			_, err = s.store.AddEvent(evt)
			if err != nil {
				// TODO: keep metric on this, add to queue of events to retry?
				fmt.Println("Error adding log event", err)
			}
		} else {
			fmt.Println("unrecognized log type, won't be added:", topic)
		}
	}
}

func (s *RsyslogServer) AcceptLogs() {
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

func (s *RsyslogServer) Stop() error {
	return s.lis.Close()
}
