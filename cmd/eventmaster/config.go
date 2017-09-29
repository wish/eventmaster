package main

import (
	"encoding/json"
	"fmt"
	"os"

	em "github.com/ContextLogic/eventmaster"
)

// EMConfig is eventmaster config that comes from the parsing of a config file.
type EMConfig struct {
	DataStore      string             `json:"data_store"`
	CassConfig     em.CassandraConfig `json:"cassandra_config"`
	UpdateInterval int                `json:"update_interval"`
}

// DefaultEMConfig returns sane defaults for an EMConfig
func DefaultEMConfig() EMConfig {
	return EMConfig{
		DataStore: "cassandra",
		CassConfig: em.CassandraConfig{
			Addrs:       []string{"127.0.0.1:9042"},
			Keyspace:    "event_master",
			Consistency: "one",
			Timeout:     "5s",
			ServiceName: "cassandra-client",
		},
		UpdateInterval: 10,
	}
}

// ParseEMConfig parses configuration out of a json file and applies settings
// over sane defaults.
//
// If file is "" then it just returns the defaults.
func ParseEMConfig(file string) (EMConfig, error) {
	r := DefaultEMConfig()
	if file == "" {
		return r, nil
	}

	f, err := os.Open(file)
	if err != nil {
		return r, fmt.Errorf("opening config file: %v", err)
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(&r); err != nil {
		return r, fmt.Errorf("json decode of file: %v", err)
	}

	return r, nil
}
