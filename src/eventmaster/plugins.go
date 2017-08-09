package main

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"time"
)

var Plugins = map[string]Plugin{
	"github": &GitHubPlugin{},
}

type Plugin interface {
	ParseRequest(*http.Request, []byte) (map[string]interface{}, error)
	ParseInfo(*http.Request, map[string]interface{}) (dc string, host string, user string, targetHostSet []string, tags []string, parentEventId string, eventTime int64, data map[string]interface{})
	ValidateAuth(*http.Request, []byte) bool
	GetTopics() []string
}

type GitHubPlugin struct{}

func (g *GitHubPlugin) ValidateAuth(r *http.Request, payload []byte) bool {
	secret := os.Getenv("GITHUB_SECRET_TOKEN")
	h := hmac.New(sha1.New, []byte(secret))
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

func (g *GitHubPlugin) ParseRequest(r *http.Request, payload []byte) (map[string]interface{}, error) {
	var info map[string]interface{}
	if err := json.Unmarshal(payload, &info); err != nil {
		return nil, err
	}
	return info, nil
}

func (g *GitHubPlugin) ParseInfo(r *http.Request, info map[string]interface{}) (dc string, host string, user string, targetHostSet []string, tags []string, parentEventId string, eventTime int64, data map[string]interface{}) {
	if eventType, ok := r.Header["X-Github-Event"]; ok {
		tags = eventType
	}
	if repoInfo, ok := info["repository"]; ok {
		if repoMap, ok := repoInfo.(map[string]interface{}); ok {
			tags = append(tags, repoMap["full_name"].(string))
		}
	}
	if pusherInfo, ok := info["pusher"]; ok {
		if pusherMap, ok := pusherInfo.(map[string]interface{}); ok {
			user = pusherMap["name"].(string)
		}
	}
	eventTime = time.Now().Unix()
	dc = "github"
	host = "github"
	data = info
	return
}

func (g *GitHubPlugin) GetTopics() []string {
	return []string{"github"}
}
