package main

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
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

func NewGitHubPlugin(conf map[string]interface{}) *GitHubPlugin {
	dc := "github"
	host := "github"
	secret := ""
	eventTopicMapping := make(map[string]string)
	if v, ok := conf["dc"]; ok {
		dc = v.(string)
	}
	if v, ok := conf["host"]; ok {
		host = v.(string)
	}
	if v, ok := conf["secret"]; ok {
		secret = v.(string)
	}
	if v, ok := conf["event_topic_mapping"]; ok {
		topicMapping := v.(map[string]interface{})
		for k, v := range topicMapping {
			eventTopicMapping[k] = v.(string)
		}
	}
	return &GitHubPlugin{
		Secret:            secret,
		Dc:                dc,
		Host:              host,
		EventTopicMapping: eventTopicMapping,
	}
}

func (g *GitHubPlugin) ValidateAuth(r *http.Request, payload []byte) bool {
	if g.Secret == "" {
		return true
	}
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
			eType := eventType[0]
			if v, ok := g.EventTopicMapping[eType]; ok {
				topicName = v
			}
		}
	}

	if repoInfo, ok := info["repository"]; ok {
		if repoMap, ok := repoInfo.(map[string]interface{}); ok {
			if fn, ok := repoMap["full_name"].(string); ok {
				tags = append(tags, fn)
			}
		}
	}
	user := ""
	if pusherInfo, ok := info["pusher"]; ok {
		if pusherMap, ok := pusherInfo.(map[string]interface{}); ok {
			user, _ = pusherMap["name"].(string)
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
