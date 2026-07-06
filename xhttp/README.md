# xhttp

Internal HTTP client used by output plugins that talk to remote HTTP services 
([elasticsearch](/plugin/output/elasticsearch/README.md), [http](/plugin/output/http/README.md),
[loki](/plugin/output/loki/README.md), [splunk](/plugin/output/splunk/README.md)). Built on top of
[fasthttp](https://github.com/valyala/fasthttp)

## Circuit breaker

Helps multi-endpoint output plugins keep sending data when some endpoints are down.
Failed endpoints are marked as banned for a while so requests stop going to them, and
get unbanned automatically after ban period.

## Ban and restore

Client keeps full and live endpoint lists. `getEndpoint` picks random endpoint from live list.

If request fails with network error or HTTP `429`, `502`, `503`, `504`, endpoint is removed from the available list and marked as "banned until `now + ban_period`".

Background goroutine runs every `reconnect_interval` and restores endpoints with expired ban period.

## All endpoints banned

`getEndpoint` doesn't return nil. Round-robins over all endpoints including banned. Banned endpoint may be back up, so trying it may succeed before restore tick. Ban stays, restore loop removes it after `ban_period`.

## Disabled circuit breaker

The breaker is off when:

* ban_period = 0
* single endpoint is configured

## Configuration

Configured via output plugin that uses xhttp.

**`ban_period`** *`duration`* *`default=10s`* 

Period for which addresses will be banned in case of unavailability.

<br>

**`reconnect_interval`** *`duration`* *`default=5s`* 

Interval for checking banned endpoints.

<br>
