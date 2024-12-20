package hash

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

type normalizer interface {
	normalize(out, data []byte) []byte
}

type reNormalizer struct {
	re       *regexp.Regexp
	matchBuf []reMatch
}

func newReNormalizer() normalizer {
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

func (n *reNormalizer) normalize(out, data []byte) []byte {
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

func formatPlaceholder(v string) []byte {
	return []byte(fmt.Sprintf("<%s>", v))
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
		name: "ip",
		re: fmt.Sprintf(`(?:%s)|(?:%s)`,
			`(?:[0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|`+
				`(?:[0-9a-fA-F]{1,4}:){1,7}:|`+
				`(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|`+
				`(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|`+
				`(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|`+
				`(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|`+
				`(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|`+
				`[0-9a-fA-F]{1,4}:(?:(?::[0-9a-fA-F]{1,4}){1,6})|`+
				`:(?:(?::[0-9a-fA-F]{1,4}){1,7}|:)|`+
				`fe80:(?::[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|`+
				`::(?:ffff(?::0{1,4}){0,1}:){0,1}`+
				`(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}`+
				`(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])|`+
				`(?:[0-9a-fA-F]{1,4}:){1,4}:`+
				`(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}`+
				`(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\b`,
			`\b(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}`+
				`(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\b`,
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
		name: "datetime",
		re: `(?:(?:Sun|Mon|Tue|Wed|Thu|Fri|Sat),\s\d{1,2}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s\d{2,4}\s\d{1,2}:\d{1,2}(?::\d{1,2})?\s(?:[-\+][\d]{2}[0-5][\d]|(?:UT|GMT|(?:E|C|M|P)(?:ST|DT)|[A-IK-Z])))|` +
			`(?:(?:(?:Sun|Mon|Tue|Wed|Thu|Fri|Sat)\s)?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s[0-3]\d,\s\d{2,4})|` +
			`(?:(?:Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),\s\d{2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{2}\s\d{2}:\d{2}:\d{2}\s(?:UT|GMT|(?:E|C|M|P)(?:ST|DT)|[A-IK-Z]))|` +
			`(?:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?(?:[+-]?\d{2}:\d{2})?)|` +
			`(?:\d{4}-?[01]\d-?[0-3]\d\s[0-2]\d:[0-5]\d:[0-5]\d)(?:\.\d+)?|` +
			`(?:\d{1,2}:\d{2}(?::\d{2})?(?:\s?[AaPp][Mm])?)|` +
			`(?:\d{4}-[01]\d-[0-3]\d)|` +
			`(?:[0-2]\d:[0-5]\d:[0-5]\d)|` +
			`(?:\b(?:(?:Sun|Mon|Tue|Wed|Thu|Fri|Sat)\s+)?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(?:[\d]{1,2})\s+(?:[\d]{2}:[\d]{2}:[\d]{2})\s+[\d]{4})|` +
			`(?:\b(?:(?:Sun|Mon|Tue|Wed|Thu|Fri|Sat),\s+)?(?:0[1-9]|[1-2]?[\d]|3[01])\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(?:19[\d]{2}|[2-9][\d]{3})\s+(?:2[0-3]|[0-1][\d]):(?:[0-5][\d])(?::(?:60|[0-5][\d]))?\s+(?:[-\+][\d]{2}[0-5][\d]|(?:UT|GMT|(?:E|C|M|P)(?:ST|DT)|[A-IK-Z])))`,
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
		re:   `-?\d+[\.,]\d+\b`,
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
