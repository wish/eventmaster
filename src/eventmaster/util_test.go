package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var getIndexTests = []struct {
	TopicId        string
	Time           int64
	ExpectedResult string
}{
	{"0fb22da2-a2c6-4d2e-a966-cae5eceff5cf", 1500333894, "0fb22da2-a2c6-4d2e-a966-cae5eceff5cf_2017_07_17"},
	{"fdedfdd5-67d8-4569-bc0e-21d7aef02f3e", 1000, "fdedfdd5-67d8-4569-bc0e-21d7aef02f3e_1970_01_01"},
	{"fdedfdd5-67d8-4569-bc0e-21d7aef02f3e", 1500333936000, "fdedfdd5-67d8-4569-bc0e-21d7aef02f3e_49513_09_10"},
}

func TestGetIndex(t *testing.T) {
	for _, test := range getIndexTests {
		assert.Equal(t, test.ExpectedResult, getIndex(test.TopicId, test.Time))
	}
}

var insertDefaultsTests = []struct {
	Schema         map[string]interface{}
	Input          map[string]interface{}
	ExpectedResult map[string]interface{}
}{
	{dataSchema["properties"].(map[string]interface{}), map[string]interface{}{
		"first_name": "alice",
		"last_name":  "smith",
		"user_id":    123,
	}, map[string]interface{}{
		"first_name": "alice",
		"last_name":  "smith",
		"user_id":    123,
	}},
	{dataSchema["properties"].(map[string]interface{}), map[string]interface{}{
		"user_id": 123,
	}, map[string]interface{}{
		"first_name": "bob",
		"user_id":    123,
	}},
}

func TestInsertDefaults(t *testing.T) {
	for _, test := range insertDefaultsTests {
		insertDefaults(test.Schema, test.Input)
		assert.Equal(t, test.ExpectedResult, test.Input)
	}
}

var backwardsCompatibleTests = []struct {
	OldSchema      map[string]interface{}
	NewSchema      map[string]interface{}
	ExpectedResult bool
}{
	{dataSchema, map[string]interface{}{
		"title": "test",
		"properties": map[string]interface{}{
			"first_name": map[string]interface{}{
				"type":    "string",
				"default": "bob",
			},
			"last_name": map[string]interface{}{
				"type": "string",
			},
		},
	}, true},
	{dataSchema, map[string]interface{}{
		"title":    "test",
		"type":     "object",
		"required": []interface{}{"user_id", "last_name"},
		"properties": map[string]interface{}{
			"first_name": map[string]interface{}{
				"type":    "string",
				"default": "bob",
			},
			"last_name": map[string]interface{}{
				"type":    "string",
				"default": "smith",
			},
			"user_id": map[string]interface{}{
				"type":    "integer",
				"minimum": 0,
			},
		},
	}, true},
	{dataSchema, map[string]interface{}{
		"title":    "test",
		"type":     "object",
		"required": []interface{}{"user_id", "some_field"},
		"properties": map[string]interface{}{
			"first_name": map[string]interface{}{
				"type":    "string",
				"default": "bob",
			},
			"last_name": map[string]interface{}{
				"type": "string",
			},
			"user_id": map[string]interface{}{
				"type":    "integer",
				"minimum": 0,
			},
		},
	}, false},
	{dataSchema, map[string]interface{}{
		"title":    "test",
		"type":     "object",
		"required": []interface{}{"user_id", "some_field"},
		"properties": map[string]interface{}{
			"first_name": map[string]interface{}{
				"type":    "string",
				"default": "bob",
			},
			"last_name": map[string]interface{}{
				"type": "string",
			},
			"user_id": map[string]interface{}{
				"type":    "integer",
				"minimum": 0,
			},
			"some_field": map[string]interface{}{
				"type": "string",
			},
		},
	}, false},
}

func TestCheckBackwardsCompatible(t *testing.T) {
	for _, test := range backwardsCompatibleTests {
		result := checkBackwardsCompatible(test.OldSchema, test.NewSchema)
		assert.Equal(t, test.ExpectedResult, result)
	}
}

var parseKeyValueTests = []struct {
	Input          string
	ExpectedResult map[string]interface{}
}{
	{"", map[string]interface{}{}},
	{"key1=value1 key2=value2 key3=value3", map[string]interface{}{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}},
}

func TestParseKeyValuePairs(t *testing.T) {
	for _, test := range parseKeyValueTests {
		assert.Equal(t, test.ExpectedResult, parseKeyValuePair(test.Input))
	}
}
