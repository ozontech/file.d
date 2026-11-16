package compiler

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
)

func TestFieldOperations(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name   string
		source string
		input  string
		want   string
		err    string
	}
	tests := []testCase{
		{
			name: "exists distinguishes missing and null",
			source: `.checks = [exists(.missing), exists(."null"), exists(.flag), exists(.text),
				exists(.items), exists(.object), exists(.)]`,
			input: `{"null":null,"flag":false,"text":"","items":[],"object":{}}`,
			want:  `{"null":null,"flag":false,"text":"","items":[],"object":{},"checks":[false,true,true,true,true,true,true]}`,
		},
		{
			name:   "exists follows typed paths",
			source: `.checks = [exists(.items[-1].value), exists(.items[2]), exists(.items["0"]), exists(.object[0]), exists(.object["0"])]`,
			input:  `{"items":[null,{"value":null}],"object":{"0":true}}`,
			want:   `{"items":[null,{"value":null}],"object":{"0":true},"checks":[true,false,false,false,true]}`,
		},
		{
			name:   "exists on incompatible parents",
			source: `.checks = [exists(.number.x), exists(."null".x), exists(.missing.x), exists(.items[-3]), exists(.number[0])]`,
			input:  `{"number":42,"null":null,"items":[1]}`,
			want:   `{"number":42,"null":null,"items":[1],"checks":[false,false,false,false,false]}`,
		},
		{
			name:   "exists on metadata",
			source: `%label = ""; .checks = [exists(%label), exists(%missing)]`,
			input:  `{}`,
			want:   `{"checks":[true,false]}`,
		},
		{
			name:   "dynamic path from event values",
			source: `.checks = [exists(.items[.index][.key]), exists(.items[.index].absent)]`,
			input:  `{"index":0,"key":"value","items":[{"value":null}]}`,
			want:   `{"index":0,"key":"value","items":[{"value":null}],"checks":[true,false]}`,
		},
		{
			name:   "move field",
			source: `.moved = rename(.old, .new)`,
			input:  `{"old":"value","keep":42}`,
			want:   `{"new":"value","keep":42,"moved":true}`,
		},
		{
			name:   "move explicit null",
			source: `.moved = rename(.old, .new)`,
			input:  `{"old":null}`,
			want:   `{"new":null,"moved":true}`,
		},
		{
			name:   "missing source creates nothing",
			source: `.moved = rename(.old, .parent.new)`,
			input:  `{}`,
			want:   `{"moved":false}`,
		},
		{
			name:   "incompatible source parent is missing",
			source: `.moved = rename(.old.value, .new)`,
			input:  `{"old":42}`,
			want:   `{"old":42,"moved":false}`,
		},
		{
			name:   "preserve collision with null",
			source: `.moved = rename(.old, .new)`,
			input:  `{"old":"value","new":null}`,
			want:   `{"old":"value","new":null,"moved":false}`,
		},
		{
			name:   "override collision",
			source: `.moved = rename(.old, .new, override: true)`,
			input:  `{"old":{"items":[1,null]},"new":"replace"}`,
			want:   `{"new":{"items":[1,null]},"moved":true}`,
		},
		{
			name:   "dynamic override",
			source: `.moved = rename(.old, .new, override: .allow)`,
			input:  `{"old":1,"new":2,"allow":true}`,
			want:   `{"new":1,"allow":true,"moved":true}`,
		},
		{
			name: "ordered fallback sources",
			source: `rename(.first, .service)
				rename(.second, .service)
				rename(.last, .service, override: true)`,
			input: `{"first":null,"second":"keep","last":"winner"}`,
			want:  `{"second":"keep","service":"winner"}`,
		},
		{
			name: "literal and nested field names",
			source: `rename(."exception.message", .flat)
				rename(.exception.message, .nested)`,
			input: `{"exception.message":"literal","exception":{"message":"nested","keep":true}}`,
			want:  `{"flat":"literal","nested":"nested","exception":{"keep":true}}`,
		},
		{
			name:   "create nested destination",
			source: `rename(.old, .parent.child.new)`,
			input:  `{"old":[1,{"x":2}]}`,
			want:   `{"parent":{"child":{"new":[1,{"x":2}]}}}`,
		},
		{
			name:   "fields inside array elements",
			source: `rename(.items[-1].old, .items[0].new)`,
			input:  `{"items":[{}, {"old":"value","keep":1}]}`,
			want:   `{"items":[{"new":"value"},{"keep":1}]}`,
		},
		{
			name:   "dynamic field names",
			source: `rename(.[.source], .[.destination])`,
			input:  `{"source":"old","destination":"new","old":"value"}`,
			want:   `{"source":"old","destination":"new","new":"value"}`,
		},
		{
			name:   "same field is a no-op",
			source: `.moved = rename(.items[-1].value, .items[0].value, override: true)`,
			input:  `{"items":[{"value":{"keep":1}}]}`,
			want:   `{"items":[{"value":{"keep":1}}],"moved":false}`,
		},
		{
			name: "preserve local source after composite move",
			source: `saved = .old
				rename(.old, .new)
				.new.nested.value = 2
				.copy = saved`,
			input: `{"old":{"nested":{"value":1},"items":[true,null]}}`,
			want:  `{"new":{"nested":{"value":2},"items":[true,null]},"copy":{"nested":{"value":1},"items":[true,null]}}`,
		},
		{
			name: "resolve arguments in source order",
			source: `key = "initial"
				.moved = rename(.object[key = "a"], .object[key = key + "b"], override: (key = key + "c") == "abc")
				.order = key`,
			input: `{"object":{"a":1}}`,
			want:  `{"object":{"ab":1},"moved":true,"order":"abc"}`,
		},
		{
			name:   "read source after option evaluation",
			source: `.moved = rename(.old, .new, override: (.old = 2) == 2)`,
			input:  `{"old":1}`,
			want:   `{"new":2,"moved":true}`,
		},
		{
			name: "named argument before paths follows source order",
			source: `key = "missing"
				rename(override: (key = "old") == "old", .[key], .new)`,
			input: `{"old":1,"new":2}`,
			want:  `{"new":1}`,
		},
		{
			name: "named argument between paths follows source order",
			source: `key = "start"
				rename(.[key = "old"], override: (key = key + "_new") == "old_new", .[key])`,
			input: `{"old":1,"old_new":2}`,
			want:  `{"old_new":1}`,
		},
		{
			name:   "bool option is captured before later index mutation",
			source: `rename(override: .allow, .[to_string(.allow = false)], .new)`,
			input:  `{"allow":true,"false":1,"new":2}`,
			want:   `{"allow":false,"new":1}`,
		},
		{
			name:   "invalid option kind is captured before later index mutation",
			source: `rename(override: .allow, .[to_string(.allow = true)], .new)`,
			input:  `{"allow":"yes","true":1}`,
			want:   `{"allow":true,"true":1}`,
			err:    `argument "override": expected bool, got string`,
		},
		{
			name:   "later path error precedes option type error",
			source: `rename(override: "yes", .old, .items[1 / 0].new)`,
			input:  `{"old":1,"items":[{}]}`,
			want:   `{"old":1,"items":[{}]}`,
			err:    "division by zero",
		},
		{
			name: "nested target and value functions",
			source: `rename(.old, .new, override: exists(.flag))
				.result = upcase(to_string(exists(.new)))`,
			input: `{"old":1,"new":2,"flag":null}`,
			want:  `{"new":1,"flag":null,"result":"TRUE"}`,
		},
		{
			name:   "option type error keeps previous effects",
			source: `.before = true; rename(.old, .new, override: "yes"); .after = true`,
			input:  `{"old":1}`,
			want:   `{"old":1,"before":true}`,
			err:    `argument "override": expected bool, got string`,
		},
		{
			name:   "option error also checked for missing source",
			source: `rename(.missing, .new, override: 1 / 0)`,
			input:  `{}`,
			want:   `{}`,
			err:    "division by zero",
		},
		{
			name:   "path evaluation error stops before option",
			source: `rename(.old[1 / 0], .new, override: (.changed = true))`,
			input:  `{"old":1}`,
			want:   `{"old":1}`,
			err:    "division by zero",
		},
		{
			name:   "reject source ancestor",
			source: `rename(.old, .old.child)`,
			input:  `{"old":{"value":1}}`,
			want:   `{"old":{"value":1}}`,
			err:    "must not contain each other",
		},
		{
			name:   "reject destination ancestor with equivalent indices",
			source: `rename(.items[-1].old.child, .items[0].old, override: true)`,
			input:  `{"items":[{"old":{"child":1}}]}`,
			want:   `{"items":[{"old":{"child":1}}]}`,
			err:    "must not contain each other",
		},
		{
			name:   "reject scalar destination parent",
			source: `rename(.old, .parent.new, override: true)`,
			input:  `{"old":1,"parent":null}`,
			want:   `{"old":1,"parent":null}`,
			err:    "destination parent is not an object",
		},
		{
			name:   "reject array destination parent",
			source: `rename(.old, .items.new)`,
			input:  `{"old":1,"items":[]}`,
			want:   `{"old":1,"items":[]}`,
			err:    "destination parent is not an object",
		},
		{
			name:   "reject missing array without creating partial parents",
			source: `rename(.old, .parent.items[0].new)`,
			input:  `{"old":1}`,
			want:   `{"old":1}`,
			err:    "destination array must already exist",
		},
		{
			name:   "reject out of bounds destination",
			source: `rename(.old, .items[1].new)`,
			input:  `{"old":1,"items":[{}]}`,
			want:   `{"old":1,"items":[{}]}`,
			err:    "destination index 1 out of bounds",
		},
		{
			name:   "reject moving array elements",
			source: `rename(.items[0], .new)`,
			input:  `{"items":[1]}`,
			want:   `{"items":[1]}`,
			err:    "path must end with an object field",
		},
		{
			name:   "reject destination array element",
			source: `rename(.old, .items[0])`,
			input:  `{"old":1,"items":[2]}`,
			want:   `{"old":1,"items":[2]}`,
			err:    "path must end with an object field",
		},
		{
			name:   "invalid dynamic path type",
			source: `exists(.items[true])`,
			input:  `{"items":[]}`,
			want:   `{"items":[]}`,
			err:    "path index must be integer or string",
		},
		{
			name:   "empty dynamic key",
			source: `exists(.[""])`,
			input:  `{}`,
			want:   `{}`,
			err:    "path field name must not be empty",
		},
		{
			name:   "empty literal key",
			source: `exists(."")`,
			input:  `{}`,
			want:   `{}`,
			err:    "path field name must not be empty",
		},
	}

	type executionMode struct {
		name     string
		prepared bool
		mapBased bool
	}
	modes := []executionMode{
		{
			name: "json/general",
		},
		{
			name:     "json/prepared",
			prepared: true,
		},
		{
			name:     "map/general",
			mapBased: true,
		},
		{
			name:     "map/prepared",
			prepared: true,
			mapBased: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					exprs := compileCalls(t, tc.source, mode.prepared)
					for range 3 {
						root, err := insaneJSON.DecodeString(tc.input)
						require.NoError(t, err)
						t.Cleanup(func() { insaneJSON.Release(root) })
						var target core.Target = runtime.NewRootTarget(root, "test", nil)
						if mode.mapBased {
							event := core.JsonNodeToValue(root.Node).(core.ObjectValue)
							target = runtime.NewMapTargetFrom(event.V)
						}
						ctx := runtime.NewContext(target, stdlib.GetRegistry())
						for _, expr := range exprs {
							_, err = expr.Eval(ctx)
							if err != nil {
								break
							}
						}
						if tc.err == "" {
							require.NoError(t, err)
						} else {
							require.ErrorContains(t, err, tc.err)
						}
						want, err := insaneJSON.DecodeString(tc.want)
						require.NoError(t, err)
						t.Cleanup(func() { insaneJSON.Release(want) })
						got, err := target.Get(core.Path{})
						require.NoError(t, err)
						require.Equal(t, core.JsonNodeToValue(want.Node), got)
					}
				})
			}
		})
	}
}

func TestFieldOperationValidation(t *testing.T) {
	t.Parallel()

	type testCase struct {
		source string
		err    string
	}
	tests := []testCase{
		{
			source: `exists()`,
			err:    `missing required argument "path"`,
		},
		{
			source: `exists(.a, .b)`,
			err:    `too many arguments`,
		},
		{
			source: `exists(path: .a)`,
			err:    `argument "path" is positional`,
		},
		{
			source: `exists("a")`,
			err:    `argument "path" must be a path`,
		},
		{
			source: `value = {}; exists(value.a)`,
			err:    `argument "path" must be a path`,
		},
		{
			source: `rename(.a)`,
			err:    `missing required argument "destination"`,
		},
		{
			source: `rename(.a, .b, true)`,
			err:    `too many arguments`,
		},
		{
			source: `rename(.a, .b, overwrite: true)`,
			err:    `unknown argument "overwrite"`,
		},
		{
			source: `rename(.a, .b, override: true, override: false)`,
			err:    `duplicate argument "override"`,
		},
		{
			source: `rename("a", .b)`,
			err:    `argument "source" must be a path`,
		},
		{
			source: `rename(.a, value)`,
			err:    `argument "destination" must be a path`,
		},
		{
			source: `rename(., .b)`,
			err:    "requires event fields",
		},
		{
			source: `rename(.a, %b)`,
			err:    "requires event fields",
		},
		{
			source: `exists(.items[unknown()])`,
			err:    `unknown function "unknown"`,
		},
		{
			source: `rename(.items[unknown()].old, .new)`,
			err:    `unknown function "unknown"`,
		},
		{
			source: `rename(.old, .items[unknown()].new)`,
			err:    `unknown function "unknown"`,
		},
		{
			source: `rename(.old, .new, override: unknown())`,
			err:    `unknown function "unknown"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.source, func(t *testing.T) {
			t.Parallel()
			compiler, err := NewCompiler(tc.source)
			require.NoError(t, err)
			exprs, err := compiler.Compile()
			if err == nil {
				err = Prepare(exprs, stdlib.GetRegistry())
			}
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestRenamePreservesNumberLiterals(t *testing.T) {
	t.Parallel()

	exprs := compileCalls(t, `rename(.number, .renamed); rename(.object, .nested); rename(.array, .items)`, true)
	_, event, err := runCalls(t, exprs, `{"number":9007199254740993,"object":{"float":1.500,"exponent":1e3},"array":[-0,18446744073709551615]}`)
	require.NoError(t, err)
	root, err := insaneJSON.DecodeString(event)
	require.NoError(t, err)
	defer insaneJSON.Release(root)
	require.Equal(t, "9007199254740993", root.Dig("renamed").AsString())
	require.Equal(t, "1.500", root.Dig("nested", "float").AsString())
	require.Equal(t, "1e3", root.Dig("nested", "exponent").AsString())
	require.Equal(t, "[-0,18446744073709551615]", root.Dig("items").EncodeToString())
}
