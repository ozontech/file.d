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
	pNo  = 0

	pCurlyBracketed  = 1 << (iota - 2) // {...}
	pSquareBracketed                   // [...]
	pParenthesized                     // (...)
	pDoubleQuoted                      // "..."
	pSingleQuoted                      // '...'
	pGraveQuoted                       // `...`
	pEmail
	pUrl
	pHost
	pUuid
	pSha1
	pMd5
	pDatetime
	pIp
	pDuration
	pHex
	pFloat
	pInt
	pBool
)

var patternById = map[string]int{
	"all": pAll,
	"no":  pNo,

	"curly_bracketed":  pCurlyBracketed,
	"square_bracketed": pSquareBracketed,
	"parenthesized":    pParenthesized,
	"double_quoted":    pDoubleQuoted,
	"single_quoted":    pSingleQuoted,
	"grave_quoted":     pGraveQuoted,
	"email":            pEmail,
	"url":              pUrl,
	"host":             pHost,
	"uuid":             pUuid,
	"sha1":             pSha1,
	"md5":              pMd5,
	"datetime":         pDatetime,
	"ip":               pIp,
	"duration":         pDuration,
	"hex":              pHex,
	"float":            pFloat,
	"int":              pInt,
	"bool":             pBool,
}

var placeholderByPattern = map[int]string{
	pCurlyBracketed:  "<curly_bracketed>",
	pSquareBracketed: "<square_bracketed>",
	pParenthesized:   "<parenthesized>",
	pDoubleQuoted:    "<double_quoted>",
	pSingleQuoted:    "<single_quoted>",
	pGraveQuoted:     "<grave_quoted>",
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
	pGraveQuoted,
}

func onlyNormalizeByBytesPatterns(mask int) bool {
	if mask <= pNo {
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
	BuiltinPatterns string
	CustomPatterns  []TokenPattern
}

type tokenNormalizer struct {
	lexer           *lexmachine.Lexer
	builtinPatterns int

	normalizeByBytes bool
}

func NewTokenNormalizer(params TokenNormalizerParams) (Normalizer, error) {
	builtinPatterns, err := parseBuiltinPatterns(params.BuiltinPatterns)
	if err != nil {
		return nil, err
	}

	n := &tokenNormalizer{
		builtinPatterns:  builtinPatterns,
		normalizeByBytes: hasPattern(builtinPatterns, normalizeByBytesPatterns...),
	}

	// only patterns without regexps
	if onlyNormalizeByBytesPatterns(builtinPatterns) && len(params.CustomPatterns) == 0 {
		return n, nil
	}

	n.lexer = lexmachine.NewLexer()

	err = initTokens(n.lexer, builtinPatterns, params.CustomPatterns)
	if err != nil {
		return nil, fmt.Errorf("failed to init tokens: %w", err)
	}

	defer func() { _ = recover() }()
	if err = n.lexer.Compile(); err != nil {
		return nil, fmt.Errorf("failed to compile lexer: %w", err)
	}

	return n, nil
}

func (n *tokenNormalizer) Normalize(out, data []byte) []byte {
	out = out[:0]

	var scanner *lexmachine.Scanner
	if n.normalizeByBytes {
		out = n.normalizeByTokenizer(out, newTokenizer(n.builtinPatterns, data))
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

func parseBuiltinPatterns(s string) (int, error) {
	// single pattern
	if p, ok := patternById[s]; ok {
		return p, nil
	}

	res := 0
	patterns := strings.Split(s, "|")
	for _, pId := range patterns {
		p, ok := patternById[pId]
		if !ok {
			return 0, fmt.Errorf("invalid pattern %q", pId)
		}
		res += p
	}
	return res, nil
}

func initTokens(lexer *lexmachine.Lexer,
	builtinPatterns int, customPatterns []TokenPattern,
) error {
	addTokens := func(patterns []TokenPattern) {
		for _, p := range patterns {
			if p.mask == 0 || builtinPatterns&p.mask != 0 {
				lexer.Add([]byte(p.RE), newToken(p.Placeholder))
			}
		}
	}

	if len(customPatterns) == 0 {
		if builtinPatterns == pNo {
			return errors.New("empty pattern list")
		}
		addTokens(builtinTokenPatterns)
		return nil
	}

	if builtinPatterns == pNo {
		addTokens(customPatterns)
		return nil
	}

	lastPatterns := make([]TokenPattern, 0)
	patterns := make([]TokenPattern, 0)
	for _, p := range customPatterns {
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

type token struct {
	placeholder string
	begin       int
	end         int
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

func (n *tokenNormalizer) normalizeByTokenizer(out []byte, tok *tokenizer) []byte {
	prevEnd := 0
	for t, end := tok.nextToken(); !end; t, end = tok.nextToken() {
		out = append(out, tok.data[prevEnd:t.begin]...)
		out = append(out, t.placeholder...)
		prevEnd = t.end
	}
	out = append(out, tok.data[prevEnd:]...)

	return out
}

func hasPattern(patterns int, masks ...int) bool {
	if patterns == pAll {
		return true
	}
	for _, m := range masks {
		if patterns&m != 0 {
			return true
		}
	}
	return false
}

type tokenizer struct {
	patterns int
	data     []byte
	pos      int

	curPattern   int
	counter      int
	startPattern int
}

func newTokenizer(patterns int, data []byte) *tokenizer {
	return &tokenizer{
		patterns: patterns,
		data:     data,
	}
}

func (t *tokenizer) nextToken() (token, bool) {
	t.curPattern = 0
	t.counter = 0
	t.startPattern = 0

	for i := t.pos; i < len(t.data); i++ {
		switch {
		case t.data[i] == '{' && hasPattern(t.patterns, pCurlyBracketed):
			t.processOpenBracket(pCurlyBracketed, i)
		case t.data[i] == '}' && hasPattern(t.patterns, pCurlyBracketed):
			if t, ok := t.processCloseBracket(pCurlyBracketed, i); ok {
				return t, false
			}
		case t.data[i] == '[' && hasPattern(t.patterns, pSquareBracketed):
			t.processOpenBracket(pSquareBracketed, i)
		case t.data[i] == ']' && hasPattern(t.patterns, pSquareBracketed):
			if t, ok := t.processCloseBracket(pSquareBracketed, i); ok {
				return t, false
			}
		case t.data[i] == '(' && hasPattern(t.patterns, pParenthesized):
			t.processOpenBracket(pParenthesized, i)
		case t.data[i] == ')' && hasPattern(t.patterns, pParenthesized):
			if t, ok := t.processCloseBracket(pParenthesized, i); ok {
				return t, false
			}
		case t.data[i] == '"' && hasPattern(t.patterns, pDoubleQuoted):
			t, shift, ok := t.processQuotes('"', pDoubleQuoted, i)
			if ok {
				return t, false
			}
			i += shift
		case t.data[i] == '\'' && hasPattern(t.patterns, pSingleQuoted):
			t, shift, ok := t.processQuotes('\'', pSingleQuoted, i)
			if ok {
				return t, false
			}
			i += shift
		case t.data[i] == '`' && hasPattern(t.patterns, pGraveQuoted):
			t, shift, ok := t.processQuotes('`', pGraveQuoted, i)
			if ok {
				return t, false
			}
			i += shift
		}
	}

	// last partial token for possible cropped data
	if t.curPattern != 0 {
		t.pos = len(t.data)
		return token{
			placeholder: placeholderByPattern[t.curPattern],
			begin:       t.startPattern,
			end:         t.pos,
		}, false
	}

	return token{}, true
}

func (t *tokenizer) processOpenBracket(pattern int, pos int) {
	if t.curPattern == 0 {
		t.curPattern = pattern
		t.counter = 1
		t.startPattern = pos
	} else if t.curPattern == pattern {
		t.counter++
	}
}

func (t *tokenizer) processCloseBracket(pattern int, pos int) (token, bool) {
	if t.curPattern != pattern {
		return token{}, false
	}

	t.counter--
	if t.counter > 0 {
		return token{}, false
	}

	t.pos = pos + 1
	return token{
		placeholder: placeholderByPattern[pattern],
		begin:       t.startPattern,
		end:         t.pos,
	}, true
}

func (t *tokenizer) processQuotes(c byte, pattern int, pos int) (token, int, bool) {
	if t.curPattern == 0 {
		t.curPattern = pattern
		t.counter = 1
		t.startPattern = pos

		// multiple quotes in a row
		for i := pos + 1; i < len(t.data) && t.data[i] == c; i++ {
			t.counter++
		}

		return token{}, t.counter - 1, false
	} else if t.curPattern == pattern {
		// skip escaped
		if pos > 0 && t.data[pos-1] == '\\' {
			return token{}, 0, false
		}

		tmp := t.counter - 1
		// multiple quotes in a row
		for i := pos + 1; i < len(t.data) && t.data[i] == c; i++ {
			tmp--
		}
		if tmp > 0 {
			return token{}, t.counter - tmp - 1, false
		}

		t.pos = pos + t.counter
		return token{
			placeholder: placeholderByPattern[pattern],
			begin:       t.startPattern,
			end:         t.pos,
		}, 0, true
	}
	return token{}, 0, false
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
