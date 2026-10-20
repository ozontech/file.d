package transformbench

import (
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/ozontech/file.d/plugin/action/modify"
)

func transformLogScenarios() []transformCallScenario {
	// Fixed grammar: a 20-byte timestamp, nonempty metadata tokens without
	// whitespace, and a message starting with neither a space nor '-'.
	// Metadata and messages contain no reserved []{}() delimiters.
	// This makes both parsing approaches equivalent for these inputs.
	var inputs []transformCallCase
	samples := []struct {
		name     string
		size     int
		time     string
		level    string
		service  string
		request  string
		duration string
		message  string
	}{
		{
			name: "short", size: 128, time: "2026-10-20T09:30:00Z",
			level: "INFO", service: "api", request: "req-42", duration: "12ms",
			message: "request completed",
		},
		{
			name: "long", size: 4096, time: "2026-10-20T09:31:00Z",
			level: "ERROR", service: "checkout", request: "req-108", duration: "250ms",
			message: "upstream unavailable",
		},
		{
			name: "unicode", size: 512, time: "2026-10-20T09:32:00Z",
			level: "WARN", service: "worker", request: "job-7", duration: "3ms",
			message: "запрос обработан",
		},
	}
	for _, sample := range samples {
		prefix := fmt.Sprintf("%s %s [%s] {%s} (%s) - ",
			sample.time, sample.level, sample.service, sample.request, sample.duration)
		message := sample.message + strings.Repeat("x", sample.size-len(prefix)-len(sample.message))
		input := transformFieldEvent(false)
		input["log"] = prefix + message
		want := maps.Clone(input)
		want["time"], want["level"], want["service"] = sample.time, sample.level, sample.service
		want["request"], want["duration"], want["message"] = sample.request, sample.duration, message
		inputs = append(inputs, transformEventCase(sample.name, input, want))
	}
	overwrite := transformCallCase{
		name: "overwrite_and_preserve_message",
		input: `{"log":"2026-10-20T09:33:00Z INFO [api] {r-1} (1ms) - kept - separator\nsecond line",
			"time":"old","level":"old","service":"old","request":"old","duration":"old","message":"old","keep":true}`,
		want: `{"log":"2026-10-20T09:33:00Z INFO [api] {r-1} (1ms) - kept - separator\nsecond line",
			"time":"2026-10-20T09:33:00Z","level":"INFO","service":"api","request":"r-1","duration":"1ms",
			"message":"kept - separator\nsecond line","keep":true}`,
	}

	pattern := `(?s)^(?P<time>\S+) (?P<level>[A-Z]+) \[(?P<service>[^]]+)\] \{(?P<request>[^}]+)\} \((?P<duration>[^)]+)\) - (?P<message>.*)$`
	regexConfig := make(modify.Config, 6)
	fields := []string{"time", "level", "service", "request", "duration", "message"}
	for i, field := range fields {
		regexConfig[field] = fmt.Sprintf("${log|re(%s,1,[%d],\"\",true)}", strconv.Quote(pattern), i+1)
	}
	// All six fields must be overwritten on failure, matching modify's
	// emptyOnNotMatched mode; stale output from an earlier event is invalid.
	misses := make([]transformCallCase, 0, 2)
	for _, sample := range []struct{ name, log string }{
		{name: "no_match", log: strings.Repeat("x", 128)},
		{name: "empty", log: ""},
	} {
		input := transformFieldEvent(false)
		input["log"] = sample.log
		want := maps.Clone(input)
		for _, field := range fields {
			input[field], want[field] = "old", ""
		}
		misses = append(misses, transformEventCase(sample.name, input, want))
	}

	parsers := []struct {
		name   string
		source string
		config *modify.Config
		misses []transformCallCase
	}{
		{
			name: "no_regex",
			source: `
				.time = before(.log, " ")
				.level = between(.log, " ", " [")
				.service = between(.log, "[", "]")
				.request = between(.log, "{", "}")
				.duration = between(.log, "(", ")")
				.message = after(.log, " - ")
			`,
			config: &modify.Config{
				"time":     `${log|cut("first",20)}`,
				"level":    `${log|trim_to("left"," ")|trim_to("right","[")|trim("all"," [")}`,
				"service":  `${log|trim_to("left","[")|trim_to("right","]")|trim("all","[]")}`,
				"request":  `${log|trim_to("left","{")|trim_to("right","}")|trim("all","{}")}`,
				"duration": `${log|trim_to("left","(")|trim_to("right",")")|trim("all","()")}`,
				"message":  `${log|trim_to("left"," - ")|trim("left"," -")}`,
			},
		},
		{
			name: "regex",
			source: `
				parts = parse_regex(
					.log,
					r'(?s)^(?P<time>\S+) (?P<level>[A-Z]+) \[(?P<service>[^]]+)\] \{(?P<request>[^}]+)\} \((?P<duration>[^)]+)\) - (?P<message>.*)$'
				)
				if parts != null {
					.time = parts.time
					.level = parts.level
					.service = parts.service
					.request = parts.request
					.duration = parts.duration
					.message = parts.message
				} else {
					.time = ""
					.level = ""
					.service = ""
					.request = ""
					.duration = ""
					.message = ""
				}
			`,
			config: &regexConfig,
			misses: misses,
		},
	}
	var scenarios []transformCallScenario
	for _, parser := range parsers {
		actions := []transformCallAction{{config: parser.config}}
		for _, profile := range []struct {
			name   string
			inputs []transformCallCase
		}{
			{name: "128B", inputs: inputs[:1]},
			{name: "4096B", inputs: inputs[1:2]},
			// One of each sample, in this order, repeated cyclically:
			// 1/3 short ASCII, 1/3 long ASCII, 1/3 512-byte Unicode.
			{name: "mixed", inputs: inputs},
		} {
			scenarios = append(scenarios, transformCallScenario{
				name:    "logs/" + parser.name + "/" + profile.name,
				source:  parser.source,
				actions: actions,
				inputs:  profile.inputs,
				cases:   append([]transformCallCase{overwrite}, parser.misses...),
			})
		}
		if len(parser.misses) != 0 {
			scenarios = append(scenarios, transformCallScenario{
				name:    "logs/" + parser.name + "/no_match/128B",
				source:  parser.source,
				actions: actions,
				inputs:  parser.misses[:1],
			})
		}
	}
	return scenarios
}
