package parser

import (
	"fmt"

	"github.com/timtadh/lexmachine/machines"
)

func Parse(input string) ([]Token, error) {
	scanner, _ := globalLexer.Scanner([]byte(input))

	var tokens []Token
	for raw, err, eos := scanner.Next(); !eos; raw, err, eos = scanner.Next() {
		if err != nil {
			if ui, ok := err.(*machines.UnconsumedInput); ok {
				return nil, fmt.Errorf(
					"unexpected character at (%d:%d): %q",
					ui.StartLine, ui.StartColumn, string(ui.Text),
				)
			}
			return nil, fmt.Errorf("unexpected parse error: %w", err)
		}
		if raw == nil {
			continue
		}
		tokens = append(tokens, raw.(Token))
	}

	return tokens, nil
}
