# Normalization

**Normalization** is the process of replacing dynamic parts of the string with placeholders.

## Patterns
**Pattern** is a pair of a *placeholder* and an *expression* in a regular expression language.

Each pattern has a *priority* that determines the placeholder in case the element falls under several patterns at once.

### Built-in patterns

We support a set of patterns out of the box.

| priority | pattern id | placeholder | examples |
|---|---|---|---|
| 1 | curly_bracketed | `<curly_bracketed>` | {a:"1",b:"2"} |
| 2 | square_bracketed | `<square_bracketed>` | [bla1, bla2] |
| 3 | parenthesized | `<parenthesized>` | (bla bla) |
| 4 | double_quoted | `<double_quoted>` | "bla bla"<br>"""bla bla""" |
| 5 | single_quoted | `<single_quoted>` | 'bla bla'<br>'''bla bla''' |
| 6 | grave_quoted | `<grave_quoted>` | \`bla bla\`<br>\`\`\`bla bla\`\`\` |
| 7 | email | `<email>` | test@host1.host2.com |
| 8 | url | `<url>` | https://some.host.com/page1?a=1<br>ws://some.host1.host2.net<br>ftp://login:pass@serv.example.com:21/ |
| 9 | host | `<host>` | www.weather.jp |
| 10 | uuid | `<uuid>` | 7c1811ed-e98f-4c9c-a9f9-58c757ff494f |
| 11 | sha1 | `<sha1>` | a94a8fe5ccb19ba61c4c0873d391e987982fbbd3 |
| 12 | md5 | `<md5>` | 098f6bcd4621d373cade4e832627b4f6 |
| 13 | datetime | `<datetime>` | 2025-01-13T10:20:40.999999Z<br>2025-01-13T10:20:40+04:00<br>2025-01-13 10:20:40<br>2025-01-13<br>10:20:40 |
| 14 | ip | `<ip>` | 1.2.3.4<br>01.102.103.104 |
| 15 | duration | `<duration>` | -1m5s<br>1w2d3h4m5s6ms7us8ns |
| 16 | hex | `<hex>` | 0x13eb85e69dfbc0758b12acdaae36287d<br>0X553026A59C |
| 17 | float | `<float>` | 100.23<br>-4.56 |
| 18 | int | `<int>` | 100<br>-200 |
| 19 | bool | `<bool>` | TRUE<br>false |

### Limitations of the RE language
We use the [lexmachine](https://github.com/timtadh/lexmachine) package to search for tokens according to the described patterns (lexical analysis).

This package doesn't support the full syntax of the RE language. For more information, see [readme](https://github.com/timtadh/lexmachine?tab=readme-ov-file#regular-expressions) section and [grammar](https://github.com/timtadh/lexmachine/blob/master/grammar) file.