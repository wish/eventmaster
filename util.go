package eventmaster

import (
	"fmt"
	"strings"
	"time"
)

type Pair struct {
	a string
	b interface{}
}

// stringify* used for mapping to cassandra inputs
func stringify(str string) string {
	if str == "" {
		return "null"
	}
	return fmt.Sprintf("'%s'", str)
}

func stringifyArr(arr []string) string {
	if len(arr) == 0 {
		return "null"
	}
	var newArr []string
	for _, str := range arr {
		newArr = append(newArr, stringify(str))
	}
	return fmt.Sprintf("{%s}", strings.Join(newArr, ","))
}

func stringifyUUID(str string) string {
	if str == "" {
		return "null"
	}
	return str
}

func getDataQueries(data map[string]interface{}) []Pair {
	var pairs []Pair
	for k, v := range data {
		m, ok := v.(map[string]interface{})
		if ok {
			nextPairs := getDataQueries(m)
			for _, pair := range nextPairs {
				pairs = append(pairs, Pair{fmt.Sprintf("%s.%s", k, pair.a), pair.b})
			}
		} else {
			pairs = append(pairs, Pair{k, v})
		}
	}
	return pairs
}

func insertDefaults(schema map[string]interface{}, m map[string]interface{}) {
	for k, v := range schema {
		s, ok := v.(map[string]interface{})
		if !ok {
			continue
		}

		if d, ok := s["default"]; ok {
			if _, ok = m[k]; !ok {
				m[k] = d
			}
		} else if property, ok := m[k]; ok {
			// property exists, check inner objects
			if innerM, ok := property.(map[string]interface{}); ok {
				innerProperties := s["properties"]
				if innerSchema, ok := innerProperties.(map[string]interface{}); ok {
					insertDefaults(innerSchema, innerM)
				}
			}
		} else if properties, ok := schema["properties"]; ok {
			// property doesn't exist, create it and check inner levels
			innerProperties := properties.(map[string]interface{})
			innerMap := make(map[string]interface{})
			insertDefaults(innerProperties, innerMap)
			if len(innerMap) != 0 {
				m[k] = innerMap
			}
		}
	}
}

func checkBackwardsCompatible(oldSchema map[string]interface{}, newSchema map[string]interface{}) bool {
	oldProperties := oldSchema["properties"]
	oldP, _ := oldProperties.(map[string]interface{})
	oldRequired := oldSchema["required"]
	oldR, _ := oldRequired.([]interface{})

	newProperties := newSchema["properties"]
	newP, _ := newProperties.(map[string]interface{})
	newRequired := newSchema["required"]
	newR, _ := newRequired.([]interface{})

	// get diff of new required properties, check if those have defaults
	for _, p := range newR {
		exists := false
		for _, oldP := range oldR {
			if oldP == p {
				exists = true
			}
		}

		if !exists {
			prop := p.(string)
			property, ok := newP[prop]
			if !ok {
				return false
			}
			if typedProperty, ok := property.(map[string]interface{}); ok {
				if _, ok := typedProperty["default"]; !ok {
					return false
				}
			} else {
				return false
			}
		}
	}

	// check backwards compatibility for all nested objects
	for k, v := range newP {
		if innerP, ok := v.(map[string]interface{}); ok {
			oldInnerP, ok := oldP[k]
			if !ok {
				continue
			}
			typedOldInnerP := oldInnerP.(map[string]interface{})
			if !checkBackwardsCompatible(typedOldInnerP, innerP) {
				return false
			}
		}
	}
	return true
}

func parseKeyValuePair(content string) map[string]interface{} {
	data := make(map[string]interface{})
	pairs := strings.Split(content, " ")
	for _, pair := range pairs {
		parts := strings.Split(pair, "=")
		if len(parts) > 1 {
			data[parts[0]] = parts[1]
		}
	}
	return data
}

func getDate(t int64) string {
	eventTime := time.Unix(t, 0).UTC()
	return fmt.Sprintf("%04d-%02d-%02d", eventTime.Year(), eventTime.Month(), eventTime.Day())
}
