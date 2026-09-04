package transform

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/transform/compiler"
	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	"go.uber.org/zap"
)

var (
	programCacheMu sync.Mutex
	programCache   = make(map[string]Program)
)

type Program struct {
	expressions []core.Expr
}

/*{ introduction
It transforms events with programs written in a small expression language.
A single `transform` action can rename, reshape, parse and delete fields — work that
otherwise takes a chain of single-purpose actions.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: transform
      source: |
        # parse lines like "INFO 2025-05-25 11:11:11,222 [shard 1] compaction - done"
        m = capture(.log, r'^(?P<level>\S+)\s+(?P<time>\S+ \S+)\s+\[(?P<shard>[^\]]+)\]\s+(?P<operation>\S+)\s+-\s+(?P<message>.+)$')
        if m != null {
          .level = m.level
          .time = m.time
          .shard = m.shard
          .operation = m.operation
          .message = m.message
          del .log
        }
    ...
```

The event `{"log":"INFO 2025-05-25 11:11:11,222 [shard 1] compaction - done"}` becomes:
```json
{
  "level": "INFO",
  "time": "2025-05-25 11:11:11,222",
  "shard": "shard 1",
  "operation": "compaction",
  "message": "done"
}
```

The program is compiled once at pipeline start; an invalid program fails fast at startup.
A runtime error (e.g. a type error on a particular event) stops the program for that
event only: the error is logged and the event continues down the pipeline, keeping
the fields that were set before the error.
}*/

/*{ language
A program is a list of expressions executed top to bottom for every event.
Expressions are separated by newlines, or by `;` when written on one line.
Comments start with `#` and last to the end of the line.

### Event fields

Paths address fields of the current event and always start with a dot:

```
.level                    # top-level field
.user.name                # nested field
."key with spaces"        # quoted field name
.items[0]                 # array element
.items[-1]                # array element, counted from the end
.items[i]                 # index from a variable
```

Reading a missing path returns `null` — it is never an error, so checks like
`if .user.name == null { ... }` are safe on any event.

Assigning to a path writes the field and creates missing parent objects on the way:

```
.a.b.c = 1                # {} -> {"a":{"b":{"c":1}}}
```

`del` removes a field (no-op when the field does not exist):

```
del .user.password
```

Path dots must be written tightly: `.a.b` is one path, while `.a .b` is a compile error.

### Values and literals

| type | literals |
|------|----------|
| integer | `42`, `0` |
| float | `3.14`, `1e10`, `1.5e-3` |
| string | `"hello"` with `\n`, `\t`, `\"` escapes |
| raw string | `s'C:\new\path'` — backslashes kept as is |
| regex | `r'\d+'` — compiled at startup |
| timestamp | `t'2024-01-15T10:30:00Z'` — RFC3339, `2006-01-02T15:04:05` or `2006-01-02` |
| bool | `true`, `false` |
| null | `null` |
| array | `[1, "two", true]` |
| object | `{level: "info", "other key": 2}` |

In conditions `null` and `false` are falsy; every other value is truthy.

### Variables

Variables hold intermediate values and live for one event:

```
name = .user.name             # read a field into a variable
parts = capture(.log, r'...') # keep a function result
.out = name                   # write it back to the event
```

Fields of object values are accessed with a dot or an index; both forms are
assignable, and arrays grow with `null`s when assigned past their end:

```
parts.level                   # same as parts["level"]
arr[0] = 1
obj.key = "value"
```

Member access dots follow the same rule as paths: `m.level` is member access,
`m .level` is a compile error.

### Operators

In order of increasing precedence:

| operators | meaning |
|-----------|---------|
| `=` | assignment, right-associative: `a = b = 1` |
| <code>&#124;&#124;</code> | logical or, short-circuit |
| `&&` | logical and, short-circuit |
| `==` `!=` | equality (integers and floats compare numerically) |
| `<` `<=` `>` `>=` | comparison of numbers, strings or timestamps |
| `+` `-` | addition, subtraction; `+` also concatenates strings |
| `*` `/` `%` | multiplication, division, modulo; division by zero is a runtime error |
| `!` `-` (unary) | negation |
| `f()` `a[i]` `a.b` | call, index, member access |

`+` concatenates only strings with strings — convert other values first:
`"code " + string(.code)`.

### Control flow

```
if .status >= 500 {
  .severity = "crit"
} else if .status >= 400 {
  .severity = "warn"
} else {
  .severity = "ok"
}

for i, item in .items {      # iterate an array; use _ to skip a variable
  .items[i] = item
}

if .level == "DEBUG" {
  abort                      # stop the program for this event
}
```

`abort` only stops the transform program — the event itself continues down the pipeline.
}*/

/*{ functions
Functions are called with positional and named arguments. Positional arguments
are required; named arguments are optional and fall back to their defaults:
`fn(value, option: "x")`.

+ `upcase(value)` — returns the string in upper case.
  ```
  .level = upcase(.level)    # "info" -> "INFO"
  ```

+ `string(value)` — converts any value to its string representation; `null`
  becomes an empty string. Use it to build strings from non-string fields:
  ```
  .msg = "code is " + string(.code)
  ```

+ `capture(value, pattern)` — matches the string against a regular expression
  and returns an object of its named groups `(?P<name>...)`, or `null` when the
  value does not match (unnamed groups are ignored):
  ```
  m = capture(.log, r'^(?P<level>\S+)\s+(?P<message>.+)$')
  if m != null {
    .level = m.level
    .message = m.message
  }
  ```

+ `after(value, separator)` — returns everything after the first occurrence of
  `separator`; the value is returned unchanged when the separator is not found.
  ```
  .message = after(.log, " - ")
  ```

+ `before(value, separator)` — returns everything before the first occurrence of
  `separator`; unchanged when not found.
  ```
  .level = before(.log, " ")
  ```

+ `between(value, open, close)` — returns the text between the first `open` and
  the following `close`; unchanged when either delimiter is not found.
  ```
  .shard = between(.log, "[", "]")
  ```
}*/

type Plugin struct {
	config           *Config
	program          Program
	logger           *zap.Logger
	pluginController pipeline.ActionPluginController
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The transformation program executed for every event.
	// > See the language reference and the list of built-in functions below.
	Source string `json:"source"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "transform",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()
	p.pluginController = params.Controller

	cacheKey := fmt.Sprintf("%s_%d", params.PipelineName, params.Index)

	programCacheMu.Lock()
	defer programCacheMu.Unlock()

	program, ok := programCache[cacheKey]
	if !ok {
		p.logger.Info("create transform compiler")
		cmp, err := compiler.NewCompiler(p.config.Source)
		if err != nil {
			p.logger.Fatal("failed to create compiler", zap.Error(err))
		}

		exprs, err := cmp.Compile()
		if err != nil {
			p.logger.Fatal("compilation error", zap.Error(err))
		}

		if err := compiler.ValidateCalls(exprs, stdlib.GetRegistry()); err != nil {
			p.logger.Fatal("validation error", zap.Error(err))
		}

		program = Program{exprs}
		programCache[cacheKey] = program
	}

	p.program = program
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	target := runtime.NewRootTarget(event.Root, event.SourceName, make(map[string]string))
	ctx := runtime.NewContext(target, stdlib.GetRegistry())

	for _, expr := range p.program.expressions {
		_, err := expr.Eval(ctx)
		if err != nil {
			if errors.Is(err, core.AbortError) {
				p.logger.Debug("transform program aborted")
				return pipeline.ActionPass
			}
			p.logger.Error("transform runtime error", zap.String("position", expr.Pos().String()), zap.Error(err))
			return pipeline.ActionPass
		}
	}

	return pipeline.ActionPass
}
