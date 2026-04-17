package main

import (
	"fmt"
	"log"

	"github.com/ozontech/file.d/plugin/action/transform"
)

func main() {
	examples := []string{
		`.message = "Hello, world!"`,
		`if .status >= 400 && .status < 500 { abort }`,
		`.user.tags[0] = "admin"`,
		`.score = (.hits * 1.5 + 10) >= 100.0`,
		`to_string(.count, base: 16)`,
		`[1, "two", true, null]`,
	}

	lex, _ := transform.NewLexer()

	for _, src := range examples {
		fmt.Printf("\n━━━ %s\n", src)

		tokens, err := lex.Tokenize(src)
		if err != nil {
			log.Printf("lexer: %v", err)
			continue
		}

		ast, err := transform.NewParser(tokens).Parse()
		if err != nil {
			log.Printf("parser: %v", err)
			continue
		}

		for _, node := range ast {
			fmt.Println(transform.DumpAST(node, 0))
		}
	}
}
