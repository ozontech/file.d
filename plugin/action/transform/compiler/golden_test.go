package compiler

import (
	"strings"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
)

// goldenCase checks that src compiles into the expected core.DumpAST rendering.
type goldenCase struct {
	name string
	src  string
	want string
}

// dumpExprs compiles src and renders every statement in core.DumpAST format.
func dumpExprs(t *testing.T, src string) string {
	t.Helper()

	exprs := compileN(t, src)
	dumps := make([]string, len(exprs))
	for i, e := range exprs {
		dumps[i] = core.DumpAST(e, 0)
	}
	return strings.Join(dumps, "\n")
}

func runGolden(t *testing.T, tests []goldenCase) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, strings.TrimSpace(tt.want), dumpExprs(t, tt.src))
		})
	}
}
