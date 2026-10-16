package compiler

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
)

func compileCalls(t *testing.T, source string, prepared bool) []core.Expr {
	t.Helper()

	exprs := compileN(t, source)
	validate := ValidateCalls
	if prepared {
		validate = Prepare
	}
	require.NoError(t, validate(exprs, stdlib.GetRegistry()))
	return exprs
}

func runCalls(t *testing.T, exprs []core.Expr, input string) (core.Value, string, error) {
	t.Helper()

	root, err := insaneJSON.DecodeString(input)
	require.NoError(t, err)
	defer insaneJSON.Release(root)
	ctx := runtime.NewContext(runtime.NewRootTarget(root, "test", nil), stdlib.GetRegistry())
	var value core.Value
	for _, expr := range exprs {
		value, err = expr.Eval(ctx)
		if err != nil {
			break
		}
	}
	if node, ok := value.(core.JSONNodeValue); ok {
		value = core.JsonNodeToValue(node.N)
	}
	return value, root.EncodeToString(), err
}

func TestPrepareMatchesGeneralCalls(t *testing.T) {
	tests := []struct {
		name   string
		source string
		input  string
		want   string
		err    string
	}{
		{
			name: "dynamic nested calls",
			source: `
				.out = before(trim_right(.value, .cutset), .separator)
				.again = trim_right(trim_right(.value, "!"), " ")
			`,
			input: `{"value":"hello-world  !!","cutset":" !","separator":"-"}`,
			want:  `{"value":"hello-world  !!","cutset":" !","separator":"-","out":"hello","again":"hello-world"}`,
		},
		{
			name:   "mixed prepared and general calls",
			source: `.out = trim_right(upcase(before(trim(to_string(.value), " "), "-")), "O")`,
			input:  `{"value":" hello-world "}`,
			want:   `{"value":" hello-world ","out":"HELL"}`,
		},
		{
			name: "dynamic substring arguments",
			source: `
				.rest = after(after(.value, .separator), .separator)
				.inner = between(.value, .open, .close)
			`,
			input: `{"value":"prefix [one] - two - three","separator":" - ","open":"[","close":"]"}`,
			want:  `{"value":"prefix [one] - two - three","separator":" - ","open":"[","close":"]","rest":"three","inner":"one"}`,
		},
		{
			name: "dynamic trims in loops and short circuit",
			source: `
				if after(.status, ":") == "ok" {
					for i, item in .items {
						.items[i] = upcase(trim_left(trim(item, .cutset), .prefix))
					}
				} else {
					.out = between(null, "[", "]")
				}
				.skip = false && trim_to(null, "!") == ""
				.keep = true || trim_to_left(null, "!") == ""
			`,
			input: `{"status":"state:ok","items":[" !!é! "," !я!! "],"cutset":" ","prefix":"!"}`,
			want:  `{"status":"state:ok","items":["É!","Я!!"],"cutset":" ","prefix":"!","skip":false,"keep":true}`,
		},
		{
			name: "dynamic trim_to delimiters",
			source: `
				.out = trim_to_right(trim_to_left(trim_to(.value, .marker), .open), .close)
			`,
			input: `{"value":"prefix |{a{b}c}| suffix","marker":"|","open":"{","close":"}"}`,
			want:  `{"value":"prefix |{a{b}c}| suffix","marker":"|","open":"{","close":"}","out":"{a{b}c}"}`,
		},
		{
			name: "conditions loops and short circuit",
			source: `
				if before(.value, "-") == "yes" {
					for i, item in .items {
						.items[i] = trim_right(item, "!")
					}
				} else {
					.out = before(null, "-")
				}
				.skip = false && before(null, "-") == ""
				.keep = true || trim_right(null, "!") == ""
			`,
			input: `{"value":"yes-go","items":["a!","b!!"]}`,
			want:  `{"value":"yes-go","items":["a","b"],"skip":false,"keep":true}`,
		},
		{
			name: "else branch and collections",
			source: `
				if false { abort } else {
					.out = [before("a-b", "-"), {v: trim_right("b!", "!")}]
				}
			`,
			input: `{}`,
			want:  `{"out":["a",{"v":"b"}]}`,
		},
		{
			name: "read and delete indexes",
			source: `
				.out = .values[before("key-rest", "-")]
				del .values[trim_right("key!", "!")]
			`,
			input: `{"values":{"key":"found"}}`,
			want:  `{"values":{},"out":"found"}`,
		},
		{
			name: "event changes between calls",
			source: `
				.out = before(.value, "-")
				.value = "second-rest"
				.next = before(.value, "-")
			`,
			input: `{"value":"first-rest"}`,
			want:  `{"value":"second-rest","out":"first","next":"second"}`,
		},
		{
			name: "empty strings and absent delimiter",
			source: `
				.empty = before("abc", "")
				.absent = before("abc", "-")
				.trim = trim_right("", "!")
				.unchanged = trim_right("abc", "")
			`,
			input: `{}`,
			want:  `{"empty":"","absent":"abc","trim":"","unchanged":"abc"}`,
		},
		{
			name:   "unicode and NUL",
			source: `.out = trim_right(before(.value, "\u0000"), "界🎯")`,
			input:  `{"value":"привет界🎯\u0000rest"}`,
			want:   `{"value":"привет界🎯\u0000rest","out":"привет"}`,
		},
		{
			name:   "missing field",
			source: `.out = before(.missing, "-")`,
			input:  `{}`,
			want:   `{}`,
			err:    `argument "value": expected string, got null`,
		},
		{
			name:   "explicit null",
			source: `.out = trim_right("a", null)`,
			input:  `{}`,
			want:   `{}`,
			err:    `argument "cutset": expected string, got null`,
		},
		{
			name:   "type checks in parameter order",
			source: `.out = before(42, true)`,
			input:  `{}`,
			want:   `{}`,
			err:    `argument "value": expected string, got integer`,
		},
		{
			name:   "materialized collection type",
			source: `.out = trim_right(.value, "!")`,
			input:  `{"value":[1]}`,
			want:   `{"value":[1]}`,
			err:    `argument "value": expected string, got array`,
		},
		{
			name:   "later evaluation error precedes type check",
			source: `.out = before(42, 1 / 0)`,
			input:  `{}`,
			want:   `{}`,
			err:    "division by zero",
		},
		{
			name:   "nested error",
			source: `.out = before(trim_right(null, "!"), "-")`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "trim_right": argument "value": expected string, got null`,
		},
		{
			name:   "after wrong value type",
			source: `.out = after(42, "-")`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "after": argument "value": expected string, got integer`,
		},
		{
			name:   "after null separator",
			source: `.out = after("a-b", null)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "after": argument "separator": expected string, got null`,
		},
		{
			name:   "between checks parameters in order",
			source: `.out = between("[a]", null, false)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "between": argument "open": expected string, got null`,
		},
		{
			name:   "between checks third argument type",
			source: `.out = between("[a]", "[", false)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "between": argument "close": expected string, got bool`,
		},
		{
			name:   "between third evaluation error precedes type checks",
			source: `.out = between(42, null, 1 / 0)`,
			input:  `{}`,
			want:   `{}`,
			err:    "division by zero",
		},
		{
			name:   "trim null cutset",
			source: `.out = trim("data", null)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "trim": argument "cutset": expected string, got null`,
		},
		{
			name:   "trim_left collection value",
			source: `.out = trim_left(.value, " ")`,
			input:  `{"value":{}}`,
			want:   `{"value":{}}`,
			err:    `function "trim_left": argument "value": expected string, got object`,
		},
		{
			name:   "upcase missing value",
			source: `.out = upcase(.missing)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "upcase": argument "value": expected string, got null`,
		},
		{
			name:   "trim_to null value",
			source: `.out = trim_to(null, "|")`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "trim_to": argument "value": expected string, got null`,
		},
		{
			name:   "trim_to_left wrong delimiter type",
			source: `.out = trim_to_left("data", 42)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "trim_to_left": argument "cutset": expected string, got integer`,
		},
		{
			name:   "trim_to_right null delimiter",
			source: `.out = trim_to_right("data", null)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "trim_to_right": argument "cutset": expected string, got null`,
		},
		{
			name: "partial writes before nested substring error",
			source: `
				.saved = upcase(trim(.value, " "))
				.out = after(between(.value, "[", null), "-")
				.after = "unreachable"
			`,
			input: `{"value":" [data] "}`,
			want:  `{"value":" [data] ","saved":"[DATA]"}`,
			err:   `function "between": argument "close": expected string, got null`,
		},
		{
			name: "partial writes on error",
			source: `
				.saved = before("a-b", "-")
				.out = trim_right(42, "!")
				.after = "unreachable"
			`,
			input: `{}`,
			want:  `{"saved":"a"}`,
			err:   `function "trim_right"`,
		},
		{
			name: "abort skips later calls",
			source: `
				.saved = trim_right("a!", "!")
				abort
				.out = before(null, "-")
			`,
			input: `{}`,
			want:  `{"saved":"a"}`,
			err:   core.AbortError.Error(),
		},
		{
			name:   "assignment index retains runtime validation",
			source: `.items[unknown()] = 1`,
			input:  `{"items":[]}`,
			want:   `{"items":[]}`,
			err:    `unknown function "unknown"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			general := compileCalls(t, tt.source, false)
			prepared := compileCalls(t, tt.source, true)
			// Reuse both programs with fresh events to catch invocation state leaks.
			for range 3 {
				wantValue, wantEvent, wantErr := runCalls(t, general, tt.input)
				gotValue, gotEvent, gotErr := runCalls(t, prepared, tt.input)
				require.Equal(t, wantValue, gotValue)
				require.JSONEq(t, tt.want, wantEvent)
				require.JSONEq(t, wantEvent, gotEvent)
				if tt.err == "" {
					require.NoError(t, wantErr)
					require.NoError(t, gotErr)
				} else {
					require.ErrorContains(t, wantErr, tt.err)
					require.EqualError(t, gotErr, wantErr.Error())
					require.Equal(t, errors.Is(wantErr, core.AbortError), errors.Is(gotErr, core.AbortError))
				}
			}
		})
	}
}

// Recording the fallback calls proves which path actually executed, rather
// than merely checking that preparation attached a plan to the AST.
type recordingCallContext struct {
	*runtime.Context
	calls []string
}

func (c *recordingCallContext) CallFunc(pos core.Position, name string, positional []core.Value, named map[string]core.Value) (core.Value, error) {
	c.calls = append(c.calls, name)
	return c.Context.CallFunc(pos, name, positional, named)
}

func TestPrepareSelectsCallPath(t *testing.T) {
	tests := []struct {
		name     string
		source   string
		prepared bool
		want     []string
	}{
		{
			name:     "general calls",
			source:   `before(trim_right(upcase("a-b!!"), "!"), "-")`,
			prepared: false,
			want:     []string{"upcase", "trim_right", "before"},
		},
		{
			name:     "prepared calls",
			source:   `before(trim_right(upcase("a-b!!"), "!"), "-")`,
			prepared: true,
			want:     nil,
		},
		{
			name:     "prepared calls with general to_string",
			source:   `before(trim_right(upcase(to_string("a-b!!")), "!"), "-")`,
			prepared: true,
			want:     []string{"to_string"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exprs := compileCalls(t, tt.source, tt.prepared)
			ctx := &recordingCallContext{Context: runtime.NewContext(nil, stdlib.GetRegistry())}

			value, err := exprs[0].Eval(ctx)

			require.NoError(t, err)
			require.Equal(t, core.StringValue{V: "A"}, value)
			require.Equal(t, tt.want, ctx.calls)
		})
	}
}

func TestPrepareValidationCompatibility(t *testing.T) {
	tests := []struct {
		name   string
		source string
		err    string
	}{
		{
			name:   "missing argument",
			source: `before("x")`,
			err:    `missing required argument "separator"`,
		},
		{
			name:   "too many arguments",
			source: `before("x", "y", "z")`,
			err:    "too many arguments",
		},
		{
			name:   "missing after separator",
			source: `after("value")`,
			err:    `missing required argument "separator"`,
		},
		{
			name:   "missing between closing delimiter",
			source: `between("value", "[")`,
			err:    `missing required argument "close"`,
		},
		{
			name:   "too many upcase arguments",
			source: `upcase("value", "extra")`,
			err:    "too many arguments",
		},
		{
			name:   "positional argument passed by name",
			source: `before(value: "x", separator: "y")`,
			err:    `argument "value" is positional and cannot be passed by name`,
		},
		{
			name:   "unknown named argument",
			source: `trim_right("x", "!", extra: true)`,
			err:    `unknown argument "extra"`,
		},
		{
			name:   "unknown argument precedes duplicate check",
			source: `trim_right("x", "!", extra: true, extra: false)`,
			err:    `unknown argument "extra"`,
		},
		{
			name:   "duplicate named argument",
			source: `slice("abc", 0, end: 1, end: 2)`,
			err:    `duplicate argument "end"`,
		},
		{
			name:   "unknown nested function",
			source: `before(unknown(), "-")`,
			err:    `unknown function "unknown"`,
		},
		{
			name:   "invalid call in unvisited branch",
			source: `if false { before("x") }`,
			err:    `missing required argument "separator"`,
		},
		{
			name:   "unknown function in read index",
			source: `.items[unknown()]`,
			err:    `unknown function "unknown"`,
		},
		{
			name:   "unknown function in delete index",
			source: `del .items[unknown()]`,
			err:    `unknown function "unknown"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			general := compileN(t, tt.source)
			prepared := compileN(t, tt.source)

			wantErr := ValidateCalls(general, stdlib.GetRegistry())
			gotErr := Prepare(prepared, stdlib.GetRegistry())

			require.ErrorContains(t, wantErr, tt.err)
			require.EqualError(t, gotErr, wantErr.Error())
		})
	}
}

func TestPreparedProgramConcurrentEvents(t *testing.T) {
	exprs := compileCalls(t, `
		value = trim(.value, " ")
		value = trim_left(value, "!")
		value = trim_to_right(value, "|")
		value = trim_to_left(value, "|")
		value = trim_to(value, "|")
		value = between(value, "|", "|")
		value = after(value, "prefix-")
		.out = upcase(before(trim_right(value, "!"), "-"))
	`, true)
	for i := range 16 {
		t.Run(fmt.Sprintf("worker_%d", i), func(t *testing.T) {
			t.Parallel()

			for j := range 20 {
				input := fmt.Sprintf(`{"value":" !!|prefix-%d:%d-rest|!! "}`, i, j)
				_, event, err := runCalls(t, exprs, input)
				require.NoError(t, err)
				want := fmt.Sprintf(`{"value":" !!|prefix-%d:%d-rest|!! ","out":"%d:%d"}`, i, j, i, j)
				require.JSONEq(t, want, event)
			}
		})
	}
}
