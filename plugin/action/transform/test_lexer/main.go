package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform"
)

func printToken(t transform.Token) {
	fmt.Printf("│  %-25s %-6d %-6d %q\n",
		transform.TokenNames[transform.TokenType(t.Type)], t.StartLine, t.StartColumn, t.Lexeme)
}

func printTokens(tokens []transform.Token) {
	fmt.Printf("│  %-25s %-5s %-5s %s\n", "TYPE", "LINE", "COLUMN", "VALUE")
	fmt.Printf("├%s\n", strings.Repeat("─", 70))
	for _, t := range tokens {
		printToken(t)
	}
}

func runExample(lexer *transform.Lexer, name, code string) {
	sep := strings.Repeat("─", 70)
	fmt.Printf("\n┌%s\n", sep)
	fmt.Printf("│  %s\n", name)
	fmt.Printf("├%s\n", sep)
	for _, line := range strings.Split(code, "\n") {
		fmt.Printf("│  %s\n", line)
	}
	fmt.Printf("├%s\n", sep)

	tokens, err := lexer.Tokenize(code)
	if err != nil {
		fmt.Printf("│   Ошибка лексера: %v\n", err)
	} else {
		printTokens(tokens)
	}
	fmt.Printf("└%s\n", sep)
}

func main() {
	lexer, err := transform.NewLexer()
	if err != nil {
		log.Fatalf("Ошибка инициализации лексера: %v", err)
	}

	examples := []struct{ name, code string }{
		{
			"Присваивание строки в поле события",
			`.message = "Hello, world!"`,
		},
		{
			"Условие if/else с abort",
			`if .level == "error" {
    abort
} else {
    .elsewhere = true
}`,
		},
		{
			"Вызов функции: ! (infallible) и ?? (null-coalesce)",
			`.parsed = parse_json!(.raw_message) ?? {}`,
		},
		{
			"Timestamp и regex литералы",
			`.created_at = t'2024-06-01T12:00:00Z'
.is_phone = match(.contact, r'^\+7\d{10}$')`,
		},
		{
			"Арифметика: float, операторы, сравнение",
			`.score = (.hits * 1.5 + 10.0e0) >= 100.0`,
		},
		{
			"Вложенный путь и индексация массива",
			`.user.roles[0] = "admin"`,
		},
		{
			"Error propagation ?, метаданные %",
			`result, err = parse_json(.body)?
.meta = %custom.source ?? "unknown"`,
		},
		{
			"Логические операторы && и ||",
			`if .status >= 400 && .status < 500 || .critical == true {
    .category = "alert"
}`,
		},
		{
			"Null-coalescing assign ??=",
			`.timeout ??= 30`,
		},
		{
			"Ошибка: неизвестный символ",
			`.x = @unknown`,
		},
	}

	for i, ex := range examples {
		runExample(lexer, fmt.Sprintf("Пример %d - %s", i+1, ex.name), ex.code)
	}
}
