package normalize

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

type reNormalizer struct {
	re       *regexp.Regexp
	matchBuf []reMatch
}

func NewReNormalizer() Normalizer {
	var sb strings.Builder
	for _, r := range defaultRegexps {
		sb.WriteString(fmt.Sprintf("(?P<%s>%s)|", r.name, r.re))
	}
	reStr := sb.String()
	reStr = reStr[:len(reStr)-1] // remove last '|'

	return &reNormalizer{
		re:       regexp.MustCompile(reStr),
		matchBuf: make([]reMatch, 0),
	}
}

func (n *reNormalizer) Normalize(out, data []byte) []byte {
	out = out[:0]
	n.matchBuf = n.matchBuf[:0]

	indexes := n.re.FindAllSubmatchIndex(data, -1)
	for _, index := range indexes {
		for i := 1; i < len(n.re.SubexpNames()); i++ {
			start := index[i*2]
			end := index[i*2+1]
			if start == -1 {
				continue
			}
			n.matchBuf = append(n.matchBuf, reMatch{
				groupIdx: i,
				start:    start,
				end:      end,
			})
		}
	}

	if len(n.matchBuf) == 0 {
		return append(out, data...)
	}

	// sort by asc
	slices.SortFunc(n.matchBuf, func(m1, m2 reMatch) int {
		return m1.start - m2.start
	})

	prevEnd := 0
	for _, m := range n.matchBuf {
		out = append(out, data[prevEnd:m.start]...)
		out = append(out, formatPlaceholder(n.re.SubexpNames()[m.groupIdx])...)
		prevEnd = m.end
	}
	out = append(out, data[prevEnd:]...)

	return out
}

type reMatch struct {
	groupIdx int
	start    int
	end      int
}

type namedRe struct {
	name string
	re   string
}

var defaultRegexps = []namedRe{
	{
		name: "email",
		re:   `[a-zA-Z0-9.!#$%&'*+/=?^_{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*`,
	},
	{
		name: "url",
		re:   `\b(?:wss?|https?|ftp)://[^\s/$.?#].[^\s]*\b`,
	},
	{
		name: "host",
		re: fmt.Sprintf(`\b(?:[a-zA-Z0-9\-]{1,63}\.)+?(?:%s|%s)\b`,
			// top 100 TLDs
			`(?:COM|NET|ORG|JP|DE|UK|FR|BR|IT|RU|ES|ME|GOV|PL|CA|AU|CN|CO|IN|NL|EDU|INFO|EU|CH|ID|AT|KR|CZ|MX|BE|TV|SE|TR|TW|AL|UA|IR|VN|CL|SK|LY|CC|TO|NO|FI|US|PT|DK|AR|HU|TK|GR|IL|NEWS|RO|MY|BIZ|IE|ZA|NZ|SG|EE|TH|IO|XYZ|PE|BG|HK|RS|LT|LINK|PH|CLUB|SI|SITE|MOBI|BY|CAT|WIKI|LA|GA|XXX|CF|HR|NG|JOBS|ONLINE|KZ|UG|GQ|AE|IS|LV|PRO|FM|TIPS|MS|SA|APP)`,
			`(?:com|net|org|jp|de|uk|fr|br|it|ru|es|me|gov|pl|ca|au|cn|co|in|nl|edu|info|eu|ch|id|at|kr|cz|mx|be|tv|se|tr|tw|al|ua|ir|vn|cl|sk|ly|cc|to|no|fi|us|pt|dk|ar|hu|tk|gr|il|news|ro|my|biz|ie|za|nz|sg|ee|th|io|xyz|pe|bg|hk|rs|lt|link|ph|club|si|site|mobi|by|cat|wiki|la|ga|xxx|cf|hr|ng|jobs|online|kz|ug|gq|ae|is|lv|pro|fm|tips|ms|sa|app)`,
		),
	},
	{
		name: "uuid",
		re:   `\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`,
	},
	{
		name: "sha1",
		re:   `\b[0-9a-fA-F]{40}\b`,
	},
	{
		name: "md5",
		re:   `\b[0-9a-fA-F]{32}\b`,
	},
	{
		// RFC3339, RFC3339Nano, DateTime, DateOnly, TimeOnly
		name: "datetime",
		re: fmt.Sprintf(`\b(?:%s)|(?:%s)|(?:%s)\b`,
			`(?:(?:\d{4}-\d{2}-\d{2})T(?:\d{2}:\d{2}:\d{2}(?:\.\d+)?))(?:Z|[\+-]\d{2}:\d{2})?`,
			`\d{2}:\d{2}:\d{2}`,
			`(?:\d{4}-\d{2}-\d{2})(?:\s{1}\d{2}:\d{2}:\d{2})?`,
		),
	},
	{
		// IPv4 only
		name: "ip",
		re:   `\b(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\b`,
	},
	{
		name: "duration",
		re:   `-?(?:(?:\d+|\d+\.\d+)(?:ns|us|µs|ms|s|m|h|d|w))+\b`,
	},
	{
		name: "hex",
		re:   `\b0[xX][0-9a-fA-F]+\b`,
	},
	{
		name: "float",
		re:   `-?\d+\.\d+\b`,
	},
	{
		name: "int",
		re:   `-?\d+\b`,
	},
	{
		name: "bool",
		re:   `\b(?:[Tt][Rr][Uu][Ee]|[Ff][Aa][Ll][Ss][Ee])\b`,
	},
}
