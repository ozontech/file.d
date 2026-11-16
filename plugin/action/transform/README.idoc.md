# Transform plugin
@introduction

### Config params
@config-params|description

## Language
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
name = .user.name                 # read a field into a variable
parts = parse_regex(.log, r'...')  # keep a function result
.out = name                       # write it back to the event
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
`"code " + to_string(.code)`.

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

## Functions
Functions are called with positional and named arguments. Positional arguments
are required; named arguments are optional and fall back to their defaults:
`fn(value, option: "x")`.

+ `exists(path)` — returns whether an event path or a metadata field exists.
  An explicit `null`, `false`, empty string, array or object still exists.
  Missing fields, incompatible parent types and out-of-bounds array indices
  return `false`; `exists(.)` returns `true`.
  ```
  if !exists(.service) {
    .service = "unknown"
  }
  exists(.items[-1].name)
  exists(.[field_name])
  exists(%source)
  ```

+ `rename(source, destination, override: false)` — moves one event field and
  returns `true` when it was moved. A missing source or identical source and
  destination returns `false`. By default, an existing destination, including
  `null`, preserves both fields and returns `false`. `override: true` replaces
  the destination. The source is removed only after a successful write.
  ```
  rename(.log, .message, override: true)
  rename(."exception.message", .exception_message)
  rename(.syslog.identifier, .service)
  rename(.k8s_label_app, .service)
  ```
  Calls execute in program order: the last two calls above give syslog priority
  when `service` is absent. Paths may contain dynamic keys or array indices,
  but both must end in object fields, such as `.items[0].name`. Moving array
  elements, the event root or metadata is not supported. Missing destination
  objects are created; existing scalar parents and missing/out-of-bounds arrays
  are errors. Neither path may contain the other. These errors leave the event
  unchanged by the rename itself.

These two operations take **paths**, not strings or local values. Quoted names
address literal keys (`."exception.message"`), while `.exception.message`
addresses a nested field. Integer indices address arrays; string keys address
objects. Arguments are evaluated in source order; for a path argument, only its
indices are evaluated. The `override` argument must be a bool. Presence checks and the move happen afterward, using the
current event. Effects of argument expressions are retained if a later argument
fails. No full-event snapshot is created. Composite values written by `rename`
are copied before deleting the source, so later destination writes do not alter
local values still referencing the removed source. Field names must be nonempty.

+ `upcase(value)` — returns the string in upper case.
  ```
  .level = upcase(.level)    # "info" -> "INFO"
  ```

+ `to_string(value)` — converts any value to its string representation; `null`
  becomes an empty string. Use it to build strings from non-string fields:
  ```
  .msg = "code is " + to_string(.code)
  ```

+ `parse_regex(value, pattern, numeric_groups: false)` — matches the string
  against a regular expression and returns an object of its named groups
  `(?P<name>...)`, or `null` when the value does not match (unnamed groups are
  ignored):
  ```
  m = parse_regex(.log, r'^(?P<level>\S+)\s+(?P<message>.+)$')
  if m != null {
    .level = m.level
    .message = m.message
  }
  ```
  With `numeric_groups: true` every group is additionally keyed by its index as a
  string — `"0"` is the whole match — which is how a pattern without named groups
  is read:
  ```
  m = parse_regex(.message, r'(\w+):.*', numeric_groups: true)
  if m != null {
    .level = m["1"]
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

+ `parse_regex_all(value, pattern, group: 0, limit: -1)` — returns every match of
  `pattern` as an array, or an empty array when nothing matches. `group` selects
  a capture group (`0` is the whole match) and a negative `limit` collects all
  occurrences:
  ```
  .ids = parse_regex_all(.log, r'id=(\w+)', group: 1)
  ```

+ `join(value, separator)` — joins an array of strings into one string. Only
  strings are joined; convert other values with `to_string()` first:
  ```
  .extracted = join(parse_regex_all(.message, r're\d+', limit: 2), ",")   # "re1,re2"
  ```

+ `trim(value, cutset)`, `trim_left(value, cutset)`,
  `trim_right(value, cutset)` — strip characters from both ends, the start or
  the end. `cutset` is a *set of characters*, not a substring: `trim_right(v, "ms")`
  removes every trailing `m` and `s`.
  ```
  .message = trim_right(.message, "\n")
  ```

+ `trim_to(value, cutset)`, `trim_to_left(value, cutset)`,
  `trim_to_right(value, cutset)` — cut the string back to a delimiter, keeping the
  delimiter itself; the value is returned unchanged when it is not found. Here
  `cutset` is a *substring*: the left side trims to its first occurrence, the
  right side to its last one, so the outermost pair is kept:
  ```
  # 'some data {"took":"200ms"} some data' -> '{"took":"200ms"}'
  .message = trim_to_right(trim_to_left(.message, "{"), "}")
  ```

+ `slice(value, start, end: null)` — returns the part of the string between two
  positions, counted in characters. Both positions may be negative to count from
  the end, and positions outside the string are clamped rather than raising an
  error. `end` defaults to the end of the string:
  ```
  .head = slice(.message, 0, end: 10)   # first 10 characters
  .tail = slice(.message, -5)           # last 5 characters
  ```

+ `lookup(value, table, default: <unchanged>)` — translates a value through a
  table of replacements. It turns enumeration codes into readable names without
  a chain of `if`s:
  ```
  api_key = {"0": "produce", "1": "fetch", "2": "offsets"}
  .kafka_request_api_key = lookup(.kafka_request_api_key, api_key)
  ```
  Keys are matched by their string form, so the number `0` and the string `"0"`
  are the same key — JSON writes codes both ways. A value that is not in the
  table is returned unchanged; pass `default:` to replace it instead:
  ```
  .severity = lookup(.status, {"500": "crit", "400": "warn"}, default: "ok")
  ```
  A table written as a literal is built once at startup, not per event, so a
  large table costs no more than a small one. Keep it in a variable when the
  same table is used more than once.
