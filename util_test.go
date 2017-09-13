package eventmaster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

var getDateTests = []struct {
	Input    int64
	Expected string
}{
	{1503521169, "2017-08-23"},
	{13219200, "1970-06-03"},
	{1758758432, "2025-09-25"},
}

func TestGetDate(t *testing.T) {
	for _, test := range getDateTests {
		assert.Equal(t, test.Expected, getDate(test.Input))
	}
}
