package transform

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/compiler"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	"github.com/stretchr/testify/require"
)

// compile and validate every program snippet shown in the plugin documentation.
func TestDocExamplesCompile(t *testing.T) {
	snippets := []string{
		// introduction example
		`m = capture(.log, r'^(?P<level>\S+)\s+(?P<time>\S+ \S+)\s+\[(?P<shard>[^\]]+)\]\s+(?P<operation>\S+)\s+-\s+(?P<message>.+)$')
		if m != null {
		  .level = m.level
		  .time = m.time
		  .shard = m.shard
		  .operation = m.operation
		  .message = m.message
		  del .log
		}`,
		// paths
		".level\n.user.name\n.\"key with spaces\"\n.items[0]\n.items[-1]\n.items[i]",
		`if .user.name == null { .x = 1 }`,
		`.a.b.c = 1`,
		`del .user.password`,
		// variables
		"name = .user.name\nparts = capture(.log, r'...')\n.out = name",
		"x = parts.level\narr = [1]\narr[0] = 1\nobj = {}\nobj.key = \"value\"",
		`a = b = 1`,
		// operators
		`.msg = "code is " + string(.code)`,
		// control flow
		`if .status >= 500 {
		  .severity = "crit"
		} else if .status >= 400 {
		  .severity = "warn"
		} else {
		  .severity = "ok"
		}

		for i, item in .items {
		  .items[i] = item
		}

		if .level == "DEBUG" {
		  abort
		}`,
		// functions
		`.level = upcase(.level)`,
		`m = capture(.log, r'^(?P<level>\S+)\s+(?P<message>.+)$')
		if m != null {
		  .level = m.level
		  .message = m.message
		}`,
		`.message = after(.log, " - ")`,
		`.level = before(.log, " ")`,
		`.shard = between(.log, "[", "]")`,
		`api_key = {"0": "produce", "1": "fetch", "2": "offsets"}
		.kafka_request_api_key = lookup(.kafka_request_api_key, api_key)`,
		`.severity = lookup(.status, {"500": "crit", "400": "warn"}, default: "ok")`,
	}

	for i, src := range snippets {
		cmp, err := compiler.NewCompiler(src)
		require.NoError(t, err, "snippet %d", i)
		exprs, err := cmp.Compile()
		require.NoError(t, err, "snippet %d", i)
		require.NoError(t, compiler.ValidateCalls(exprs, stdlib.GetRegistry()), "snippet %d", i)
	}
}
