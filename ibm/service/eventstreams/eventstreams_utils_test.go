// Copyright IBM Corp. 2026 All Rights Reserved.
// Licensed under the Mozilla Public License v2.0

package eventstreams

import (
	"errors"
	"strings"
	"testing"

	"github.com/IBM/platform-services-go-sdk/resourcecontrollerv2"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatObject(t *testing.T) {
	testCases := []struct {
		name     string
		input    map[string]interface{}
		expected string
	}{
		{
			name:     "empty map",
			input:    map[string]interface{}{},
			expected: "",
		},
		{
			name: "flat map with primitive types",
			input: map[string]interface{}{
				"string_val": "foo",
				"int_val":    123,
				"bool_val":   true,
				"float_val":  45.67,
			},
			expected: `"bool_val":bool,"float_val":float64,"int_val":int,"string_val":string`,
		},
		{
			name: "nested map",
			input: map[string]interface{}{
				"a": "hello",
				"nested": map[string]interface{}{
					"count": 10,
					"flag":  false,
				},
			},
			expected: `"a":string,"nested":{"count":int,"flag":bool}`,
		},
		{
			name: "deeply nested map",
			input: map[string]interface{}{
				"level1": map[string]interface{}{
					"level2": map[string]interface{}{
						"leaf": "value",
					},
				},
			},
			expected: `"level1":{"level2":{"leaf":string}}`,
		},
		{
			name: "map with slice and nil values",
			input: map[string]interface{}{
				"slice_val": []string{"a", "b"},
				"nil_val":   nil,
			},
			expected: `"nil_val":<nil>,"slice_val":[]string`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := formatObject(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestParseInstanceExtensions(t *testing.T) {
	t.Run("gen1: valid kafka_http_url and kafka_brokers_sasl", func(t *testing.T) {
		instance := &resourcecontrollerv2.ResourceInstance{
			Extensions: map[string]interface{}{
				"kafka_http_url": "https://tenant1.example.com",
				"kafka_brokers_sasl": []interface{}{
					"broker-c.example.com:9093",
					"broker-a.example.com:9093",
					"broker-b.example.com:9093",
				},
			},
		}
		ext, err := parseInstanceExtensions(instance, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, ext.platformGeneration)
		assert.Equal(t, "https://tenant1.example.com", ext.adminURL)
		wantServers := []string{
			"broker-a.example.com:9093",
			"broker-b.example.com:9093",
			"broker-c.example.com:9093",
		}
		require.Len(t, ext.bootstrapServers, len(wantServers))
		assert.Equal(t, wantServers, ext.bootstrapServers)
	})

	t.Run("gen2: valid dataservices connection block", func(t *testing.T) {
		instance := &resourcecontrollerv2.ResourceInstance{
			Extensions: map[string]interface{}{
				"dataservices": map[string]any{
					"connection": map[string]any{
						"bootstrap_servers": "broker-1.example.com:9093,broker-2.example.com:9093",
						"rest_url":          "https://admin.example.com",
					},
				},
			},
		}
		ext, err := parseInstanceExtensions(instance, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, ext.platformGeneration)
		assert.Equal(t, "https://admin.example.com", ext.adminURL)
		wantServers := strings.Split("broker-1.example.com:9093,broker-2.example.com:9093", ",")
		require.Len(t, ext.bootstrapServers, len(wantServers))
		assert.Equal(t, wantServers, ext.bootstrapServers)
	})

	t.Run("error: empty extensions", func(t *testing.T) {
		instance := &resourcecontrollerv2.ResourceInstance{
			Extensions: map[string]interface{}{},
		}
		ext, err := parseInstanceExtensions(instance, nil)
		require.Error(t, err)
		assert.Nil(t, ext)
	})

	t.Run("error: kafka_http_url present but kafka_brokers_sasl missing", func(t *testing.T) {
		instance := &resourcecontrollerv2.ResourceInstance{
			Extensions: map[string]interface{}{
				"kafka_http_url": "https://tenant1.example.com",
			},
		}
		ext, err := parseInstanceExtensions(instance, nil)
		require.Error(t, err)
		assert.Nil(t, ext)
	})

	t.Run("error: dataservices present but rest_url missing", func(t *testing.T) {
		instance := &resourcecontrollerv2.ResourceInstance{
			Extensions: map[string]interface{}{
				"dataservices": map[string]any{
					"connection": map[string]any{
						"bootstrap_servers": "broker-1.example.com:9093",
						// rest_url intentionally omitted
					},
				},
			},
		}
		ext, err := parseInstanceExtensions(instance, nil)
		require.Error(t, err)
		assert.Nil(t, ext)
	})

	t.Run("error: dataservices present but bootstrap_servers missing", func(t *testing.T) {
		instance := &resourcecontrollerv2.ResourceInstance{
			Extensions: map[string]interface{}{
				"dataservices": map[string]any{
					"connection": map[string]any{
						// bootstrap_servers intentionally omitted
						"rest_url": "https://admin.example.com",
					},
				},
			},
		}
		ext, err := parseInstanceExtensions(instance, nil)
		require.Error(t, err)
		assert.Nil(t, ext)
	})
}

func TestConvertNameToMatch(t *testing.T) {
	c := &quotaClientSaramaWrapper{}

	t.Run("default maps to QuotaMatchDefault with empty match string", func(t *testing.T) {
		match, matchType := c.convertNameToMatch("default")
		assert.Equal(t, "", match)
		assert.Equal(t, sarama.QuotaMatchDefault, matchType)
	})

	t.Run("named user maps to QuotaMatchExact with the name as match string", func(t *testing.T) {
		match, matchType := c.convertNameToMatch("id-123")
		assert.Equal(t, "id-123", match)
		assert.Equal(t, sarama.QuotaMatchExact, matchType)
	})
}

func TestBuildQuotaOptions(t *testing.T) {
	c := &quotaClientSaramaWrapper{}
	cbr := int64(1000)
	pbr := int64(2000)
	rem := int64(-1)

	t.Run("both rates positive: two set ops", func(t *testing.T) {
		ops := c.buildQuotaOptions(QuotaDetails{ConsumerByteRate: &cbr, ProducerByteRate: &pbr})
		require.Len(t, ops, 2)
		assert.Equal(t, consumerByteRateKey, ops[0].Key)
		assert.InEpsilon(t, float64(1000), ops[0].Value, 1e-9)
		assert.False(t, ops[0].Remove)
		assert.Equal(t, producerByteRateKey, ops[1].Key)
		assert.InEpsilon(t, float64(2000), ops[1].Value, 1e-9)
		assert.False(t, ops[1].Remove)
	})

	t.Run("both rates -1: two remove ops", func(t *testing.T) {
		ops := c.buildQuotaOptions(QuotaDetails{ConsumerByteRate: &rem, ProducerByteRate: &rem})
		require.Len(t, ops, 2)
		assert.Equal(t, consumerByteRateKey, ops[0].Key)
		assert.True(t, ops[0].Remove)
		assert.Equal(t, producerByteRateKey, ops[1].Key)
		assert.True(t, ops[1].Remove)
	})

	t.Run("only consumer set, producer nil: one consumer op", func(t *testing.T) {
		ops := c.buildQuotaOptions(QuotaDetails{ConsumerByteRate: &cbr})
		require.Len(t, ops, 1)
		assert.Equal(t, consumerByteRateKey, ops[0].Key)
		assert.InEpsilon(t, float64(1000), ops[0].Value, 1e-9)
	})

	t.Run("producer set, consumer -1: one producer set op, one consumer remove op", func(t *testing.T) {
		ops := c.buildQuotaOptions(QuotaDetails{ConsumerByteRate: &rem, ProducerByteRate: &pbr})
		require.Len(t, ops, 2)
		assert.Equal(t, consumerByteRateKey, ops[0].Key)
		assert.True(t, ops[0].Remove)
		assert.Equal(t, producerByteRateKey, ops[1].Key)
		assert.InEpsilon(t, float64(2000), ops[1].Value, 1e-9)
		assert.False(t, ops[1].Remove)
	})

	t.Run("both rates nil: no ops", func(t *testing.T) {
		ops := c.buildQuotaOptions(QuotaDetails{})
		assert.Empty(t, ops)
	})
}

func TestQuotaNotFoundError(t *testing.T) {
	c := &quotaClientSaramaWrapper{}
	err := c.quotaNotFoundError("error response")
	require.NotNil(t, err)
	assert.True(t, err.notFound)
	assert.Contains(t, err.Error(), "quota not found")
	assert.Contains(t, *err.response, "error response")
}

func TestQuotaClientError(t *testing.T) {
	t.Run("Error() returns the wrapped error message", func(t *testing.T) {
		e := &quotaClientError{response: new("response"), err: errors.New("inner error")}
		assert.Equal(t, "inner error", e.Error())
	})
}

func TestQuotaClientIsNotFound(t *testing.T) {
	t.Run("true for quotaClientError with notFound=true", func(t *testing.T) {
		e := &quotaClientError{notFound: true, err: errors.New("x")}
		assert.True(t, quotaClientIsNotFound(e))
	})

	t.Run("false for quotaClientError with notFound=false", func(t *testing.T) {
		e := &quotaClientError{notFound: false, err: errors.New("x")}
		assert.False(t, quotaClientIsNotFound(e))
	})

	t.Run("false for a plain error", func(t *testing.T) {
		assert.False(t, quotaClientIsNotFound(errors.New("plain")))
	})
}

func TestQuotaClientResponse(t *testing.T) {
	t.Run("returns the response string when set", func(t *testing.T) {
		resp := "detailed message"
		e := &quotaClientError{response: &resp, err: errors.New("inner")}
		assert.Equal(t, "detailed message", quotaClientResponse(e))
	})

	t.Run("returns JSON-encoded error message when response is nil", func(t *testing.T) {
		e := &quotaClientError{err: errors.New("error message")}
		result := quotaClientResponse(e)
		assert.Contains(t, result, "error message")
	})

	t.Run("returns empty JSON object for a plain error", func(t *testing.T) {
		assert.Equal(t, "{}", quotaClientResponse(errors.New("plain")))
	})
}
