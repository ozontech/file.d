package transform

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/compiler"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	"github.com/stretchr/testify/require"
)

// validate examples from both the documentation source and the generated README
func TestReadmeExamplesCompile(t *testing.T) {
	for _, path := range []string{"README.idoc.md", "README.md"} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			require.NoError(t, err)

			blocks := strings.Split(string(data), "```")
			count := 0
			for i := 1; i < len(blocks); i += 2 {
				// unlabelled code blocks contain transform programs
				if !strings.HasPrefix(blocks[i], "\n") {
					continue
				}
				count++
				t.Run(fmt.Sprintf("example_%d", count), func(t *testing.T) {
					cmp, err := compiler.NewCompiler(blocks[i])
					require.NoError(t, err)
					exprs, err := cmp.Compile()
					require.NoError(t, err)
					require.NoError(t, compiler.ValidateCalls(exprs, stdlib.GetRegistry()))
				})
			}
			require.Positive(t, count, "documentation must contain program examples")
		})
	}
}

// compile and validate every program snippet shown in the plugin documentation.
func TestDocExamplesCompile(t *testing.T) {
	snippets := []string{
		// introduction example
		`m = parse_regex(.log, r'^(?P<level>\S+)\s+(?P<time>\S+ \S+)\s+\[(?P<shard>[^\]]+)\]\s+(?P<operation>\S+)\s+-\s+(?P<message>.+)$')
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
		"name = .user.name\nparts = parse_regex(.log, r'...')\n.out = name",
		"x = parts.level\narr = [1]\narr[0] = 1\nobj = {}\nobj.key = \"value\"",
		`a = b = 1`,
		// operators
		`.msg = "code is " + to_string(.code)`,
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
		`m = parse_regex(.log, r'^(?P<level>\S+)\s+(?P<message>.+)$')
		if m != null {
		  .level = m.level
		  .message = m.message
		}`,
		`.message = after(.log, " - ")`,
		`.level = before(.log, " ")`,
		`.shard = between(.log, "[", "]")`,
		`m = parse_regex(.message, r'(\w+):.*', numeric_groups: true)
		if m != null {
		  .level = m["1"]
		}`,
		`.ids = parse_regex_all(.log, r'id=(\w+)', group: 1)`,
		`.extracted = join(parse_regex_all(.message, r're\d+', limit: 2), ",")`,
		`.message = trim_right(.message, "\n")`,
		`.message = trim_to_right(trim_to_left(.message, "{"), "}")`,
		`.head = slice(.message, 0, end: 10)
		.tail = slice(.message, -5)`,
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
