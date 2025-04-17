package normalize

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/timtadh/lexmachine"
	"github.com/timtadh/lexmachine/machines"
)

const (
	patternPriorityFirst = "first"
	patternPriorityLast  = "last"
)

const (
	pAll = -1

	pCurlyBracketed  = 0x1  // {...}
	pSquareBracketed = 0x2  // [...]
	pParenthesized   = 0x4  // (...)
	pDoubleQuoted    = 0x8  // "..."
	pSingleQuoted    = 0x10 // '...'
	pEmail           = 0x20
	pUrl             = 0x40
	pHost            = 0x80
	pUuid            = 0x100
	pSha1            = 0x200
	pMd5             = 0x400
	pDatetime        = 0x800
	pIp              = 0x1000
	pDuration        = 0x2000
	pHex             = 0x4000
	pFloat           = 0x8000
	pInt             = 0x10000
	pBool            = 0x20000
)

var placeholderByPattern = map[int]string{
	pCurlyBracketed:  "<curly_bracketed>",
	pSquareBracketed: "<square_bracketed>",
	pParenthesized:   "<parenthesized>",
	pDoubleQuoted:    "<double_quoted>",
	pSingleQuoted:    "<single_quoted>",
	pEmail:           "<email>",
	pUrl:             "<url>",
	pHost:            "<host>",
	pUuid:            "<uuid>",
	pSha1:            "<sha1>",
	pMd5:             "<md5>",
	pDatetime:        "<datetime>",
	pIp:              "<ip>",
	pDuration:        "<duration>",
	pHex:             "<hex>",
	pFloat:           "<float>",
	pInt:             "<int>",
	pBool:            "<bool>",
}

var normalizeByBytesPatterns = []int{
	pCurlyBracketed,
	pSquareBracketed,
	pParenthesized,
	pDoubleQuoted,
	pSingleQuoted,
}

func normalizeByBytesPatternsMask() int {
	sum := 0
	for _, p := range normalizeByBytesPatterns {
		sum += p
	}
	return sum
}

func onlyNormalizeByBytesPatterns(mask int) bool {
	if mask <= 0 {
		return false
	}

	for p := range placeholderByPattern {
		if idx := slices.Index(normalizeByBytesPatterns, p); idx != -1 {
			continue
		}
		if mask&p != 0 {
			return false
		}
	}
	return true
}

type TokenPattern struct {
	Placeholder string
	RE          string
	Priority    string // for custom only

	mask int // for built-in only
}

type TokenNormalizerParams struct {
	BuiltinPatterns int
	CustomPatterns  []TokenPattern
}

type tokenNormalizer struct {
	lexer           *lexmachine.Lexer
	builtinPatterns int
}

func NewTokenNormalizer(params TokenNormalizerParams) (Normalizer, error) {
	// only patterns without regexps
	if onlyNormalizeByBytesPatterns(params.BuiltinPatterns) && len(params.CustomPatterns) == 0 {
		return &tokenNormalizer{
			builtinPatterns: params.BuiltinPatterns,
		}, nil
	}

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
		lexer:           l,
		builtinPatterns: params.BuiltinPatterns,
	}, nil
}

func (n *tokenNormalizer) Normalize(out, data []byte) []byte {
	out = out[:0]

	var scanner *lexmachine.Scanner
	if n.hasPattern(normalizeByBytesPatterns...) {
		out = n.normalizeByBytes(out, data)
		if n.lexer == nil {
			return out
		}

		scanner, _ = n.lexer.Scanner(out)

		// scanner copied buffer, so we can reset it
		out = out[:0]
	} else {
		scanner, _ = n.lexer.Scanner(data)
	}

	return n.normalizeByScanner(out, scanner)
}

func (n *tokenNormalizer) normalizeByScanner(out []byte, scanner *lexmachine.Scanner) []byte {
	prevEnd := 0
	for tokRaw, err, eos := scanner.Next(); !eos; tokRaw, err, eos = scanner.Next() {
		if ui, is := err.(*machines.UnconsumedInput); is {
			scanner.TC = ui.FailTC // skip
			continue
		} else if err != nil {
			out = out[:0]
			return append(out, scanner.Text...)
		}

		tok := tokRaw.(token)

		out = append(out, scanner.Text[prevEnd:tok.begin]...)
		out = append(out, tok.placeholder...)
		prevEnd = tok.end
	}
	out = append(out, scanner.Text[prevEnd:]...)

	return out
}

func (n *tokenNormalizer) normalizeByBytes(out, data []byte) []byte {
	lastPos := 0
	nextToken := func() (token, bool) {
		curPattern := 0
		counter := 0
		startPattern := 0

		openBracket := func(pattern int, pos int) {
			if curPattern == 0 {
				curPattern = pattern
				counter = 1
				startPattern = pos
			} else if curPattern == pattern {
				counter++
			}
		}
		closeBracket := func(pattern int, pos int) (token, bool) {
			if curPattern != pattern {
				return token{}, false
			}

			counter--
			if counter > 0 {
				return token{}, false
			}

			lastPos = pos + 1
			return token{
				placeholder: placeholderByPattern[pattern],
				begin:       startPattern,
				end:         pos + 1,
			}, true
		}
		quotes := func(c byte, pattern int, pos int) (token, int, bool) {
			if curPattern == 0 {
				curPattern = pattern
				counter = 1
				startPattern = pos

				// multiple quotes in a row
				for j := pos + 1; j < len(data) && data[j] == c; j++ {
					counter++
				}

				return token{}, counter - 1, false
			} else if curPattern == pattern {
				tmp := counter - 1
				// multiple quotes in a row
				for j := pos + 1; j < len(data) && data[j] == c; j++ {
					tmp--
				}
				if tmp > 0 {
					return token{}, counter - tmp - 1, false
				}

				lastPos = pos + counter
				return token{
					placeholder: placeholderByPattern[pattern],
					begin:       startPattern,
					end:         pos + counter,
				}, 0, true
			}
			return token{}, 0, false
		}

		for i := lastPos; i < len(data); i++ {
			switch {
			case n.hasPattern(pCurlyBracketed) && data[i] == '{':
				openBracket(pCurlyBracketed, i)
			case n.hasPattern(pCurlyBracketed) && data[i] == '}':
				if t, ok := closeBracket(pCurlyBracketed, i); ok {
					return t, false
				}
			case n.hasPattern(pSquareBracketed) && data[i] == '[':
				openBracket(pSquareBracketed, i)
			case n.hasPattern(pSquareBracketed) && data[i] == ']':
				if t, ok := closeBracket(pSquareBracketed, i); ok {
					return t, false
				}
			case n.hasPattern(pParenthesized) && data[i] == '(':
				openBracket(pParenthesized, i)
			case n.hasPattern(pParenthesized) && data[i] == ')':
				if t, ok := closeBracket(pParenthesized, i); ok {
					return t, false
				}
			case n.hasPattern(pDoubleQuoted) && data[i] == '"':
				t, shift, ok := quotes('"', pDoubleQuoted, i)
				if ok {
					return t, false
				}
				i += shift
			case n.hasPattern(pSingleQuoted) && data[i] == '\'':
				t, shift, ok := quotes('\'', pSingleQuoted, i)
				if ok {
					return t, false
				}
				i += shift
			}
		}

		// last partial token for possible cropped data
		if curPattern != 0 {
			lastPos = len(data)
			return token{
				placeholder: placeholderByPattern[curPattern],
				begin:       startPattern,
				end:         len(data),
			}, false
		}

		return token{}, true
	}

	prevEnd := 0
	for t, end := nextToken(); !end; t, end = nextToken() {
		out = append(out, data[prevEnd:t.begin]...)
		out = append(out, t.placeholder...)
		prevEnd = t.end
	}
	out = append(out, data[prevEnd:]...)

	return out
}

func (n *tokenNormalizer) hasPattern(masks ...int) bool {
	if n.builtinPatterns == pAll {
		return true
	}
	for _, m := range masks {
		if n.builtinPatterns&m != 0 {
			return true
		}
	}
	return false
}

type token struct {
	placeholder string
	begin       int
	end         int
}

func initTokens(lexer *lexmachine.Lexer, params TokenNormalizerParams) error {
	addTokens := func(patterns []TokenPattern) {
		for _, p := range patterns {
			if p.mask == 0 || params.BuiltinPatterns&p.mask != 0 {
				lexer.Add([]byte(p.RE), newToken(p.Placeholder))
			}
		}
	}

	if len(params.CustomPatterns) == 0 {
		if params.BuiltinPatterns == 0 {
			return errors.New("empty pattern list")
		}
		addTokens(builtinTokenPatterns)
		return nil
	}

	if params.BuiltinPatterns == 0 {
		addTokens(params.CustomPatterns)
		return nil
	}

	lastPatterns := make([]TokenPattern, 0)
	patterns := make([]TokenPattern, 0)
	for _, p := range params.CustomPatterns {
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
		Placeholder: placeholderByPattern[pEmail],
		RE:          `[0-9a-zA-Z_\.\-\*]+@[0-9a-zA-Z_\-]+(\.[0-9a-zA-Z_\-]+)*`,

		mask: pEmail,
	},
	{
		Placeholder: placeholderByPattern[pUrl],
		RE:          `(wss?|https?|ftp)://[0-9a-zA-Z_\.\-@:%\+~#=\?/]+`,

		mask: pUrl,
	},
	{
		Placeholder: placeholderByPattern[pHost],
		RE: fmt.Sprintf(`([0-9a-zA-Z_\-]+\.)+(%s|%s)`,
			// top 100 TLDs
			`COM|NET|ORG|JP|DE|UK|FR|BR|IT|RU|ES|ME|GOV|PL|CA|AU|CN|CO|IN|NL|EDU|INFO|EU|CH|ID|AT|KR|CZ|MX|BE|TV|SE|TR|TW|AL|UA|IR|VN|CL|SK|LY|CC|TO|NO|FI|US|PT|DK|AR|HU|TK|GR|IL|NEWS|RO|MY|BIZ|IE|ZA|NZ|SG|EE|TH|IO|XYZ|PE|BG|HK|RS|LT|LINK|PH|CLUB|SI|SITE|MOBI|BY|CAT|WIKI|LA|GA|XXX|CF|HR|NG|JOBS|ONLINE|KZ|UG|GQ|AE|IS|LV|PRO|FM|TIPS|MS|SA|APP`,
			`com|net|org|jp|de|uk|fr|br|it|ru|es|me|gov|pl|ca|au|cn|co|in|nl|edu|info|eu|ch|id|at|kr|cz|mx|be|tv|se|tr|tw|al|ua|ir|vn|cl|sk|ly|cc|to|no|fi|us|pt|dk|ar|hu|tk|gr|il|news|ro|my|biz|ie|za|nz|sg|ee|th|io|xyz|pe|bg|hk|rs|lt|link|ph|club|si|site|mobi|by|cat|wiki|la|ga|xxx|cf|hr|ng|jobs|online|kz|ug|gq|ae|is|lv|pro|fm|tips|ms|sa|app`,
		),

		mask: pHost,
	},
	{
		Placeholder: placeholderByPattern[pUuid],
		RE: fmt.Sprintf(`%s-%s-%s-%s-%s`,
			strings.Repeat(`[0-9a-fA-F]`, 8),
			strings.Repeat(`[0-9a-fA-F]`, 4),
			strings.Repeat(`[0-9a-fA-F]`, 4),
			strings.Repeat(`[0-9a-fA-F]`, 4),
			strings.Repeat(`[0-9a-fA-F]`, 12),
		),

		mask: pUuid,
	},
	{
		Placeholder: placeholderByPattern[pSha1],
		RE:          strings.Repeat(`[0-9a-fA-F]`, 40),

		mask: pSha1,
	},
	{
		Placeholder: placeholderByPattern[pMd5],
		RE:          strings.Repeat(`[0-9a-fA-F]`, 32),

		mask: pMd5,
	},
	{
		// RFC3339, RFC3339Nano, DateTime, DateOnly, TimeOnly
		Placeholder: placeholderByPattern[pDatetime],
		RE: fmt.Sprintf(`(%s)|(%s)|(%s)`,
			`\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(Z|[\+\-]\d\d:\d\d)`,
			`\d\d:\d\d:\d\d`,
			`\d\d\d\d-\d\d-\d\d( \d\d:\d\d:\d\d)?`,
		),

		mask: pDatetime,
	},
	{
		// IPv4 only
		Placeholder: placeholderByPattern[pIp],
		RE:          strings.TrimSuffix(strings.Repeat(`(25[0-5]|(2[0-4]|1?[0-9])?[0-9])\.`, 4), `\.`),

		mask: pIp,
	},
	{
		Placeholder: placeholderByPattern[pDuration],
		RE:          `-?((\d+|\d+\.\d+)(ns|us|µs|ms|s|m|h|d|w))+`,

		mask: pDuration,
	},
	{
		Placeholder: placeholderByPattern[pHex],
		RE:          `0[xX][0-9a-fA-F]+`,

		mask: pHex,
	},
	{
		Placeholder: placeholderByPattern[pFloat],
		RE:          `-?\d+\.\d+`,

		mask: pFloat,
	},
	{
		Placeholder: placeholderByPattern[pInt],
		RE:          `-?\d+`,

		mask: pInt,
	},
	{
		Placeholder: placeholderByPattern[pBool],
		RE:          `[Tt][Rr][Uu][Ee]|[Ff][Aa][Ll][Ss][Ee]`,

		mask: pBool,
	},
}
