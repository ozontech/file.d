package normalize

import (
	"errors"
	"fmt"
	"strings"

	"github.com/timtadh/lexmachine"
	"github.com/timtadh/lexmachine/machines"
)

const (
	patternPriorityFirst = "first"
	patternPriorityLast  = "last"
)

type TokenPattern struct {
	Placeholder string
	RE          string
	Priority    string
}

type TokenNormalizerParams struct {
	WithBuiltinPatterns bool
	Patterns            []TokenPattern
}

type tokenNormalizer struct {
	lexer *lexmachine.Lexer
}

func NewTokenNormalizer(params TokenNormalizerParams) (Normalizer, error) {
	l := lexmachine.NewLexer()

	err := initTokens(l, params)
	if err != nil {
		return nil, fmt.Errorf("failed to init tokens: %w", err)
	}

	defer func() { _ = recover() }()
	if err = l.Compile(); err != nil {
		return nil, fmt.Errorf("failed to compile lexer: %w", err)
	}

	return &tokenNormalizer{
		lexer: l,
	}, nil
}

func (n *tokenNormalizer) Normalize(out, data []byte) []byte {
	out = out[:0]

	scanner, _ := n.lexer.Scanner(data)
	prevEnd := 0
	for tokRaw, err, eos := scanner.Next(); !eos; tokRaw, err, eos = scanner.Next() {
		if ui, is := err.(*machines.UnconsumedInput); is {
			scanner.TC = ui.FailTC // skip
			continue
		} else if err != nil {
			out = out[:0]
			return append(out, data...)
		}

		tok := tokRaw.(token)

		out = append(out, data[prevEnd:tok.begin]...)
		out = append(out, tok.placeholder...)
		prevEnd = tok.end
	}
	out = append(out, data[prevEnd:]...)

	return out
}

type token struct {
	placeholder string
	begin       int
	end         int
}

func initTokens(lexer *lexmachine.Lexer, params TokenNormalizerParams) error {
	addTokens := func(patterns []TokenPattern) {
		for _, p := range patterns {
			lexer.Add([]byte(p.RE), newToken(p.Placeholder))
		}
	}

	if len(params.Patterns) == 0 {
		if !params.WithBuiltinPatterns {
			return errors.New("empty pattern list")
		}
		addTokens(builtinTokenPatterns)
		return nil
	}

	if !params.WithBuiltinPatterns {
		addTokens(params.Patterns)
		return nil
	}

	lastPatterns := make([]TokenPattern, 0)
	patterns := make([]TokenPattern, 0)
	for _, p := range params.Patterns {
		if p.Priority == patternPriorityFirst {
			patterns = append(patterns, p)
		} else {
			lastPatterns = append(lastPatterns, p)
		}
	}
	patterns = append(patterns, builtinTokenPatterns...)
	patterns = append(patterns, lastPatterns...)

	addTokens(patterns)
	return nil
}

func newToken(placeholder string) lexmachine.Action {
	return func(s *lexmachine.Scanner, m *machines.Match) (any, error) {
		// skip `\w<match>\w`
		if m.TC > 0 && isWord(s.Text[m.TC-1]) ||
			m.TC+len(m.Bytes) < len(s.Text) && isWord(s.Text[m.TC+len(m.Bytes)]) {
			return nil, nil
		}

		return token{
			placeholder: placeholder,
			begin:       m.TC,
			end:         m.TC + len(m.Bytes),
		}, nil
	}
}

func isWord(c byte) bool {
	return '0' <= c && c <= '9' ||
		'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' ||
		c == '_'
}

// [lexmachine] pkg doesn't support 'exactly' re syntax (a{3}, a{3,6}),
// so we use [strings.Repeat] instead
var builtinTokenPatterns = []TokenPattern{
	{
		Placeholder: "<email>",
		RE:          `[0-9a-zA-Z_\.\-]+@[0-9a-zA-Z_\-]+(\.[0-9a-zA-Z_\-]+)*`,
	},
	{
		Placeholder: "<url>",
		RE:          `(wss?|https?|ftp)://[0-9a-zA-Z_\.\-@:%\+~#=\?/]+`,
	},
	{
		Placeholder: "<host>",
		RE: fmt.Sprintf(`([0-9a-zA-Z_\-]+\.)+(%s|%s)`,
			// top 100 TLDs
			`COM|NET|ORG|JP|DE|UK|FR|BR|IT|RU|ES|ME|GOV|PL|CA|AU|CN|CO|IN|NL|EDU|INFO|EU|CH|ID|AT|KR|CZ|MX|BE|TV|SE|TR|TW|AL|UA|IR|VN|CL|SK|LY|CC|TO|NO|FI|US|PT|DK|AR|HU|TK|GR|IL|NEWS|RO|MY|BIZ|IE|ZA|NZ|SG|EE|TH|IO|XYZ|PE|BG|HK|RS|LT|LINK|PH|CLUB|SI|SITE|MOBI|BY|CAT|WIKI|LA|GA|XXX|CF|HR|NG|JOBS|ONLINE|KZ|UG|GQ|AE|IS|LV|PRO|FM|TIPS|MS|SA|APP`,
			`com|net|org|jp|de|uk|fr|br|it|ru|es|me|gov|pl|ca|au|cn|co|in|nl|edu|info|eu|ch|id|at|kr|cz|mx|be|tv|se|tr|tw|al|ua|ir|vn|cl|sk|ly|cc|to|no|fi|us|pt|dk|ar|hu|tk|gr|il|news|ro|my|biz|ie|za|nz|sg|ee|th|io|xyz|pe|bg|hk|rs|lt|link|ph|club|si|site|mobi|by|cat|wiki|la|ga|xxx|cf|hr|ng|jobs|online|kz|ug|gq|ae|is|lv|pro|fm|tips|ms|sa|app`,
		),
	},
	{
		Placeholder: "<uuid>",
		RE: fmt.Sprintf(`%s-%s-%s-%s-%s`,
			strings.Repeat(`[0-9a-fA-F]`, 8),
			strings.Repeat(`[0-9a-fA-F]`, 4),
			strings.Repeat(`[0-9a-fA-F]`, 4),
			strings.Repeat(`[0-9a-fA-F]`, 4),
			strings.Repeat(`[0-9a-fA-F]`, 12),
		),
	},
	{
		Placeholder: "<sha1>",
		RE:          strings.Repeat(`[0-9a-fA-F]`, 40),
	},
	{
		Placeholder: "<md5>",
		RE:          strings.Repeat(`[0-9a-fA-F]`, 32),
	},
	{
		// RFC3339, RFC3339Nano, DateTime, DateOnly, TimeOnly
		Placeholder: "<datetime>",
		RE: fmt.Sprintf(`(%s)|(%s)|(%s)`,
			`\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(Z|[\+\-]\d\d:\d\d)`,
			`\d\d:\d\d:\d\d`,
			`\d\d\d\d-\d\d-\d\d( \d\d:\d\d:\d\d)?`,
		),
	},
	{
		// IPv4 only
		Placeholder: "<ip>",
		RE:          strings.TrimSuffix(strings.Repeat(`(25[0-5]|(2[0-4]|1?[0-9])?[0-9])\.`, 4), `\.`),
	},
	{
		Placeholder: "<duration>",
		RE:          `-?((\d+|\d+\.\d+)(ns|us|µs|ms|s|m|h|d|w))+`,
	},
	{
		Placeholder: "<hex>",
		RE:          `0[xX][0-9a-fA-F]+`,
	},
	{
		Placeholder: "<float>",
		RE:          `-?\d+\.\d+`,
	},
	{
		Placeholder: "<int>",
		RE:          `-?\d+`,
	},
	{
		Placeholder: "<bool>",
		RE:          `[Tt][Rr][Uu][Ee]|[Ff][Aa][Ll][Ss][Ee]`,
	},
}
