package main

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"
)

var Plugins = map[string]Plugin{}

type Plugin interface {
	ParseRequest(*http.Request, []byte) ([]*UnaddedEvent, error)
	ValidateAuth(*http.Request, []byte) bool
}

type GitHubPlugin struct {
	Secret            string            `json:"secret"`
	Dc                string            `json:"dc"`
	Host              string            `json:"host"`
	EventTopicMapping map[string]string `json:"event_topic_mapping"`
}

func NewGitHubPlugin() error {
	confFile, err := ioutil.ReadFile("plugins/github.json")
	if err != nil {
		return err
	}
	var gp GitHubPlugin
	if err = json.Unmarshal(confFile, &gp); err != nil {
		return err
	}
	if gp.Dc == "" {
		gp.Dc = "github"
	}
	if gp.Host == "" {
		gp.Host = "github"
	}
	Plugins["github"] = &gp
	return nil
}

func (g *GitHubPlugin) ValidateAuth(r *http.Request, payload []byte) bool {
	h := hmac.New(sha1.New, []byte(g.Secret))
	h.Write(payload)

	if signatures, ok := r.Header["X-Hub-Signature"]; ok {
		if len(signatures) > 0 {
			sig := []byte(signatures[0][5:])
			rawSig := h.Sum(nil)
			expectedSig := make([]byte, hex.EncodedLen(len(rawSig)))
			hex.Encode(expectedSig, rawSig)
			return subtle.ConstantTimeCompare(expectedSig, sig) == 1
		}
	}
	return false
}

func (g *GitHubPlugin) ParseRequest(r *http.Request, payload []byte) ([]*UnaddedEvent, error) {
	var info map[string]interface{}
	if err := json.Unmarshal(payload, &info); err != nil {
		return nil, err
	}
	var tags []string
	topicName := "github"
	if eventType, ok := r.Header["X-Github-Event"]; ok {
		tags = eventType
		if g.EventTopicMapping != nil && len(eventType) > 0 {
			topicName = g.EventTopicMapping[eventType[0]]
		}
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
	return []*UnaddedEvent{
		&UnaddedEvent{
			TopicName: topicName,
			Dc:        g.Dc,
			Host:      g.Host,
			EventTime: time.Now().Unix(),
			Data:      info,
			Tags:      tags,
			User:      user,
		},
	}, nil
}
