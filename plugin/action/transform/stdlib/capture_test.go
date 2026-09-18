package stdlib

import (
	"regexp"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func callCapture(value, pattern string) (core.Value, error) {
	return callCaptureNamed(value, pattern, nil)
}

func callCaptureNamed(value, pattern string, named map[string]core.Value) (core.Value, error) {
	re := regexp.MustCompile(pattern)
	return callFn(capture{}, []core.Value{
		core.StringValue{V: value},
		core.RegexValue{V: re},
	}, named)
}

func TestCapture(t *testing.T) {
	t.Parallel()

	t.Run("scylla_line", func(t *testing.T) {
		t.Parallel()

		line := `INFO 2025-05-25 18:18:18,180 [shard 4:comp] compaction - [Compact xxxx] Compacted 4 sstables`
		pattern := `^(?P<level>\S+)\s+(?P<date>\d{4}-\d{2}-\d{2})\s+(?P<time>[\d:,]+)\s+\[(?P<shard>[^\]]*)\]\s+(?P<operation>\S+)\s+-\s+(?P<message>.*)$`

		got, err := callCapture(line, pattern)
		require.NoError(t, err)
		obj, ok := got.(core.ObjectValue)
		require.True(t, ok)

		assert.Equal(t, core.StringValue{V: "INFO"}, obj.V["level"])
		assert.Equal(t, core.StringValue{V: "2025-05-25"}, obj.V["date"])
		assert.Equal(t, core.StringValue{V: "18:18:18,180"}, obj.V["time"])
		assert.Equal(t, core.StringValue{V: "shard 4:comp"}, obj.V["shard"])
		assert.Equal(t, core.StringValue{V: "compaction"}, obj.V["operation"])
		assert.Equal(t, core.StringValue{V: "[Compact xxxx] Compacted 4 sstables"}, obj.V["message"])
	})

	t.Run("cassandra_line", func(t *testing.T) {
		t.Parallel()

		line := `INFO [3-19] 2025-05-25 11:11:11,999 NoSpamLogger.java:104 - /11.111.111.111:2222 failed to connect`
		pattern := `^(?P<level>\S+)\s+\[(?P<operation>[^\]]*)\]\s+(?P<date>\d{4}-\d{2}-\d{2})\s+(?P<time>[\d:,]+)\s+\S+\s+-\s+(?P<message>.*)$`

		got, err := callCapture(line, pattern)
		require.NoError(t, err)
		obj, ok := got.(core.ObjectValue)
		require.True(t, ok)

		assert.Equal(t, core.StringValue{V: "INFO"}, obj.V["level"])
		assert.Equal(t, core.StringValue{V: "3-19"}, obj.V["operation"])
		assert.Equal(t, core.StringValue{V: "2025-05-25"}, obj.V["date"])
		assert.Equal(t, core.StringValue{V: "11:11:11,999"}, obj.V["time"])
		assert.Equal(t, core.StringValue{V: "/11.111.111.111:2222 failed to connect"}, obj.V["message"])
	})

	t.Run("no_match_returns_null", func(t *testing.T) {
		t.Parallel()

		got, err := callCapture("nothing here", `^(?P<level>INFO)$`)
		require.NoError(t, err)
		assert.Equal(t, core.NullValue{}, got)
	})

	t.Run("unnamed_groups_ignored", func(t *testing.T) {
		t.Parallel()

		got, err := callCapture("abc123", `(?P<letters>[a-z]+)(\d+)`)
		require.NoError(t, err)
		obj, ok := got.(core.ObjectValue)
		require.True(t, ok)

		assert.Equal(t, core.StringValue{V: "abc"}, obj.V["letters"])
		assert.Len(t, obj.V, 1, "unnamed group must not appear in the result")
	})
}

func TestCaptureNumericGroups(t *testing.T) {
	t.Parallel()

	enabled := map[string]core.Value{"numeric_groups": core.BoolValue{V: true}}

	t.Run("positional_groups_are_keyed_by_index", func(t *testing.T) {
		t.Parallel()

		// modify README: ${message|re("service=(\S+) exec took (\d+\.?\d*(?:ms|s|m|h))",-1,[2],",")}
		got, err := callCaptureNamed(
			"service=service-test-1 exec took 200ms",
			`service=(\S+) exec took (\d+\.?\d*(?:ms|s|m|h))`,
			enabled,
		)
		require.NoError(t, err)
		obj, ok := got.(core.ObjectValue)
		require.True(t, ok)

		assert.Equal(t, core.StringValue{V: "service=service-test-1 exec took 200ms"}, obj.V["0"],
			`"0" is the whole match`)
		assert.Equal(t, core.StringValue{V: "service-test-1"}, obj.V["1"])
		assert.Equal(t, core.StringValue{V: "200ms"}, obj.V["2"])
	})

	t.Run("named_groups_are_keyed_both_ways", func(t *testing.T) {
		t.Parallel()

		got, err := callCaptureNamed("abc123", `(?P<letters>[a-z]+)(\d+)`, enabled)
		require.NoError(t, err)
		obj, ok := got.(core.ObjectValue)
		require.True(t, ok)

		assert.Equal(t, core.StringValue{V: "abc"}, obj.V["letters"])
		assert.Equal(t, core.StringValue{V: "abc"}, obj.V["1"])
		assert.Equal(t, core.StringValue{V: "123"}, obj.V["2"], "unnamed groups become reachable")
	})

	t.Run("off_by_default", func(t *testing.T) {
		t.Parallel()

		got, err := callCapture("abc123", `(?P<letters>[a-z]+)(\d+)`)
		require.NoError(t, err)
		obj, ok := got.(core.ObjectValue)
		require.True(t, ok)

		assert.NotContains(t, obj.V, "0")
		assert.NotContains(t, obj.V, "1")
	})

	t.Run("no_match_still_returns_null", func(t *testing.T) {
		t.Parallel()

		got, err := callCaptureNamed("nothing here", `^(INFO)$`, enabled)
		require.NoError(t, err)
		assert.Equal(t, core.NullValue{}, got)
	})
}
