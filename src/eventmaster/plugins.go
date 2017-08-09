package main

import (
	"encoding/json"
)

var Plugins = map[string]Plugin{
	"github": &GitHubPlugin{},
}

type Plugin interface {
	ParseRawData([]byte) (*UnaddedEvent, error)
}

type GitHubPlugin struct{}

func (g *GitHubPlugin) ParseRawData(rawData []byte) (*UnaddedEvent, error) {
	var info map[string]interface{}
	if err := json.Unmarshal(rawData, &info); err != nil {
		return nil, err
	}
	return &UnaddedEvent{
		Dc:        "github",
		Host:      "github",
		TopicName: "github",
		Data:      info,
	}, nil
}
