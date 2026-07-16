package stdlib

import (
	"regexp"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func callCapture(value, pattern string) (core.Value, error) {
	re := regexp.MustCompile(pattern)
	return capture{}.Call(map[string]core.Value{
		"value":   core.StringValue{V: value},
		"pattern": core.RegexValue{V: re},
	})
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
