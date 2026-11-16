package stdlib

import (
	"fmt"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// ValidateCall checks argument structure and path parameters before execution.
// Value kinds remain runtime checks because arguments can be expressions.
func (c *CompiledFunc) ValidateCall(pos core.Position, args []core.Argument) error {
	positionalParams := c.params[:c.numPositional]
	namedParams := c.params[c.numPositional:]
	var positionalCount int
	seen := make(map[string]bool)
	for _, arg := range args {
		if arg.Name == "" {
			positionalCount++
			continue
		}
		if seen[arg.Name] {
			return fmt.Errorf("%s: function %q: duplicate argument %q", pos, c.Name(), arg.Name)
		}
		seen[arg.Name] = true
		if containsName(namedParams, arg.Name) {
			continue
		}
		if containsName(positionalParams, arg.Name) {
			return fmt.Errorf("%s: function %q: argument %q is positional and cannot be passed by name", pos, c.Name(), arg.Name)
		}
		return fmt.Errorf("%s: function %q: unknown argument %q", pos, c.Name(), arg.Name)
	}
	if positionalCount > c.numPositional {
		return fmt.Errorf("%s: function %q: too many arguments: expected at most %d, got %d", pos, c.Name(), c.numPositional, positionalCount)
	}
	if positionalCount < c.numPositional {
		return fmt.Errorf("%s: function %q: missing required argument %q", pos, c.Name(), positionalParams[positionalCount].Name)
	}
	positional := 0
	for _, arg := range args {
		if arg.Name != "" {
			continue
		}
		param := positionalParams[positional]
		positional++
		if param.Mode != PathArgument {
			continue
		}
		path, ok := arg.Value.(*core.PathExpr)
		if !ok {
			return fmt.Errorf("%s: function %q: argument %q must be a path", pos, c.Name(), param.Name)
		}
		if param.EventFieldOnly && (path.Root != core.EventRoot || len(path.Segments) == 0) {
			return fmt.Errorf("%s: function %q: argument %q requires event fields, not root or metadata paths", pos, c.Name(), param.Name)
		}
	}
	return nil
}
