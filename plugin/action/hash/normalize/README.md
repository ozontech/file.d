# Normalization

**Normalization** is the process of replacing dynamic parts of the string with placeholders.

## Patterns
**Pattern** is a pair of a *placeholder* and an *expression* in a regular expression language.

Each pattern has a *priority* that determines the placeholder in case the element falls under several patterns at once.

### Built-in patterns

We support a set of patterns out of the box.

| pattern | priority | placeholder | examples |
|---|---|---|---|
| email | 1 | `<email>` | test@host1.host2.com |
| url | 2 | `<url>` | https://some.host.com/page1?a=1<br>ws://some.host1.host2.net<br>ftp://login:pass@serv.example.com:21/ |
| host | 3 | `<host>` | www.weather.jp |
| uuid | 4 | `<uuid>` | 7c1811ed-e98f-4c9c-a9f9-58c757ff494f |
| sha1 | 5 | `<sha1>` | a94a8fe5ccb19ba61c4c0873d391e987982fbbd3 |
| md5 | 6 | `<md5>` | 098f6bcd4621d373cade4e832627b4f6 |
| datetime | 7 | `<datetime>` | 2025-01-13T10:20:40.999999Z<br>2025-01-13T10:20:40+04:00<br>2025-01-13 10:20:40<br>2025-01-13<br>10:20:40 |
| ip (only IPv4) | 8 | `<ip>` | 1.2.3.4<br>01.102.103.104 |
| duration | 9 | `<duration>` | -1m5s<br>1w2d3h4m5s6ms7us8ns |
| hex | 10 | `<hex>` | 0x13eb85e69dfbc0758b12acdaae36287d<br>0X553026A59C |
| float | 11 | `<float>` | 100.23<br>-4.56 |
| int | 12 | `<int>` | 100<br>-200 |
| bool | 13 | `<bool>` | TRUE<br>false |

### Limitations of the RE language
We use the [lexmachine](https://github.com/timtadh/lexmachine) package to search for tokens according to the described patterns (lexical analysis).

This package doesn't support the full syntax of the RE language. For more information, see [readme](https://github.com/timtadh/lexmachine?tab=readme-ov-file#regular-expressions) section and [grammar](https://github.com/timtadh/lexmachine/blob/master/grammar) file.