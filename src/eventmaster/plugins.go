package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

var Plugins = map[string]Plugin{
	"github": &GitHubPlugin{},
}

type Plugin interface {
	ParseRequest(r *http.Request) (*UnaddedEvent, error)
}

type GitHubPlugin struct{}

func (g *GitHubPlugin) ParseRequest(r *http.Request) (*UnaddedEvent, error) {
	headers := r.Header
	var tags []string
	if eventType, ok := headers["X-Github-Event"]; ok {
		tags = eventType
	}
	rawData, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	var info map[string]interface{}
	if err := json.Unmarshal(rawData, &info); err != nil {
		return nil, err
	}
	if repoInfo, ok := info["repository"]; ok {
		if repoMap, ok := repoInfo.(map[string]interface{}); ok {
			tags = append(tags, repoMap["full_name"].(string))
		}
	}
	user := ""
	if pusherInfo, ok := info["pusher"]; ok {
		if pusherMap, ok := pusherInfo.(map[string]interface{}); ok {
			user = pusherMap["name"].(string)
		}
	}
	return &UnaddedEvent{
		Dc:        "github",
		Host:      "github",
		TopicName: "github",
		Tags:      tags,
		User:      user,
		Data:      info,
	}, nil
}
