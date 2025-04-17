# Normalization

**Normalization** is the process of replacing dynamic parts of the string with placeholders.

## Patterns
**Pattern** is a pair of a *placeholder* and an *expression* in a regular expression language.

Each pattern has a *priority* that determines the placeholder in case the element falls under several patterns at once.

### Built-in patterns

We support a set of patterns out of the box.

| priority | pattern | mask value | placeholder | examples |
|---|---|---|---|---|
| 1 | curly bracketed | 1 | `<curly_bracketed>` | {a:"1",b:"2"} |
| 2 | square bracketed | 2 | `<square_bracketed>` | [bla1, bla2] |
| 3 | parenthesized | 4 | `<parenthesized>` | (bla bla) |
| 4 | double quoted | 8 | `<double_quoted>` | "bla bla" |
| 5 | single quoted | 16 | `<single_quoted>` | 'bla bla' |
| 6 | email | 32 | `<email>` | test@host1.host2.com |
| 7 | url | 64 | `<url>` | https://some.host.com/page1?a=1<br>ws://some.host1.host2.net<br>ftp://login:pass@serv.example.com:21/ |
| 8 | host | 128 | `<host>` | www.weather.jp |
| 9 | uuid | 256 | `<uuid>` | 7c1811ed-e98f-4c9c-a9f9-58c757ff494f |
| 10 | sha1 | 512 | `<sha1>` | a94a8fe5ccb19ba61c4c0873d391e987982fbbd3 |
| 11 | md5 | 1024 | `<md5>` | 098f6bcd4621d373cade4e832627b4f6 |
| 12 | datetime | 2048 | `<datetime>` | 2025-01-13T10:20:40.999999Z<br>2025-01-13T10:20:40+04:00<br>2025-01-13 10:20:40<br>2025-01-13<br>10:20:40 |
| 13 | ip (only IPv4) | 4096 | `<ip>` | 1.2.3.4<br>01.102.103.104 |
| 14 | duration | 8192 | `<duration>` | -1m5s<br>1w2d3h4m5s6ms7us8ns |
| 15 | hex | 16384 | `<hex>` | 0x13eb85e69dfbc0758b12acdaae36287d<br>0X553026A59C |
| 16 | float | 32768 | `<float>` | 100.23<br>-4.56 |
| 17 | int | 65536 | `<int>` | 100<br>-200 |
| 18 | bool | 131072 | `<bool>` | TRUE<br>false |

### Limitations of the RE language
We use the [lexmachine](https://github.com/timtadh/lexmachine) package to search for tokens according to the described patterns (lexical analysis).

This package doesn't support the full syntax of the RE language. For more information, see [readme](https://github.com/timtadh/lexmachine?tab=readme-ov-file#regular-expressions) section and [grammar](https://github.com/timtadh/lexmachine/blob/master/grammar) file.