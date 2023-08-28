package rename

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_Remove(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	config := Config{
		"key1", "val1",
		"key2", "val2",
		"key3", "val3",
		"key4", "val4",
		"key5", "val5",
		"key6", "val6",
		"key7", "val7",
	}

	config.Remove("key7") // remove the last element
	expected := Config{
		"key1", "val1",
		"key2", "val2",
		"key3", "val3",
		"key4", "val4",
		"key5", "val5",
		"key6", "val6"}
	r.Equal(expected, config)

	config.Remove("key1") // remove the first element
	expected = Config{
		"key2", "val2",
		"key3", "val3",
		"key4", "val4",
		"key5", "val5",
		"key6", "val6"}
	r.Equal(expected, config)

	config.Remove("key4") // remove the element in the middle
	expected = Config{
		"key2", "val2",
		"key3", "val3",
		"key5", "val5",
		"key6", "val6"}
	r.Equal(expected, config)

	config.Remove("val2") // remove a not existing element
	expected = Config{
		"key2", "val2",
		"key3", "val3",
		"key5", "val5",
		"key6", "val6"}
	r.Equal(expected, config)
}

func TestConfig_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	const jsonRaw = `{"key1": "value", "key2": true, "key3": 42, "key4": false}`

	var config Config

	require.NoError(t, json.Unmarshal([]byte(jsonRaw), &config))
	require.Equal(t, []string{"key1", "value", "key2", "true", "key3", "42", "key4", "false"}, []string(config))
}
