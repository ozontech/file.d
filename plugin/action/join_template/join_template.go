package join_template

import (
	"strings"
	"unicode"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/join"
)

/*{ introduction
Alias to "join" plugin with predefined `start` and `continue` parameters.

> ⚠ Parsing the whole event flow could be very CPU intensive because the plugin uses regular expressions.
> Consider `match_fields` parameter to process only particular events. Check out an example for details.

**Example of joining Go panics**:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: join_template
      template: go_panic
      field: log
      match_fields:
        stream: stderr // apply only for events which was written to stderr to save CPU time
    ...
```
}*/

type joinTemplates map[string]struct {
	startRePat    string
	continueRePat string
}

var templates = joinTemplates{
	"go_panic": {
		startRePat:    "/^(panic:)|(http: panic serving)|^(fatal error:)/",
		continueRePat: "/(^\\s*$)|(goroutine [0-9]+ \\[)|(\\.go:[0-9]+)|(created by .*\\/?.*\\.)|(^\\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)|([A-Za-z_]+[A-Za-z0-9_]*\\)?\\.[A-Za-z0-9_]+\\(.*\\))/",
	},
}

type Plugin struct {
	config *Config

	jp *join.Plugin
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field which will be checked for joining with each other.
	Field  cfg.FieldSelector `json:"field" default:"log" required:"true" parse:"selector"` // *
	Field_ []string

	// > @3@4@5@6
	// >
	// > Max size of the resulted event. If it is set and the event exceeds the limit, the event will be truncated.
	MaxEventSize int `json:"max_event_size" default:"0"` // *

	// > @3@4@5@6
	// >
	// > The name of the template. Available templates: `go_panic`.
	Template string `json:"template" required:"true"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "join_template",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	templateName := p.config.Template
	template, ok := templates[templateName]
	if !ok {
		logger.Fatalf("join template \"%s\" not found", templateName)
	}

	startRe, err := cfg.CompileRegex(template.startRePat)
	if err != nil {
		logger.Fatalf("failed to compile regex for template \"%s\": %s", templateName, err.Error())
	}
	continueRe, err := cfg.CompileRegex(template.continueRePat)
	if err != nil {
		logger.Fatalf("failed to compile regex for template \"%s\": %s", templateName, err.Error())
	}

	jConfig := &join.Config{
		Field_:       p.config.Field_,
		MaxEventSize: p.config.MaxEventSize,
		Start_:       startRe,
		Continue_:    continueRe,

		FastCheck:          true,
		StartCheckFunc_:    goPanicStartCheck,
		ContinueCheckFunc_: goPanicContinueCheck,
	}
	p.jp = &join.Plugin{}
	p.jp.Start(jConfig, params)
}

func (p *Plugin) Stop() {
	p.jp.Stop()
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	return p.jp.Do(event)
}

func goPanicStartCheck(s string) bool {
	return strings.HasPrefix(s, "panic:") ||
		strings.Contains(s, "http: panic serving") ||
		strings.HasPrefix(s, "fatal error:")
}

func checkOnlySpaces(s string) bool {
	for _, c := range s {
		if !unicode.IsSpace(c) {
			return false
		}
	}

	return true
}

const (
	checkGoroutineIDPrefix = "goroutine "
	checkGoroutineIDSuffix = " ["
)

// may be error: not only first occurrence counts
func checkGoroutineID(s string) bool {
	i := strings.Index(s, checkGoroutineIDPrefix)
	if i == -1 {
		return false
	}

	s = s[i+len(checkGoroutineIDPrefix):]

	i = strings.Index(s, checkGoroutineIDSuffix)
	if i == -1 {
		return false
	}

	// no goroutine id found
	if i == 0 {
		return false
	}

	s = s[:i]
	return checkOnlyDigits(s)
}

func checkOnlyDigits(s string) bool {
	for _, c := range s {
		if !unicode.IsDigit(c) {
			return false
		}
	}

	return true
}

func goPanicContinueCheck(s string) bool {
	return checkOnlySpaces(s) ||
		checkGoroutineID(s) ||
		checkLineNumberAndFile(s) ||
		checkCreatedBy(s) ||
		strings.HasPrefix(s, "[signal") ||
		checkPanicAddress(s) ||
		strings.Contains(s, "panic:") ||
		checkMethodCall(s)
}

const lineSuffix = ".go:"

// may be error: not only first occurrence counts
func checkLineNumberAndFile(s string) bool {
	i := strings.Index(s, lineSuffix)
	if i == -1 {
		return false
	}

	s = s[i+len(lineSuffix):]

	i = 0
	for ; i < len(s) && unicode.IsDigit(rune(s[i])); i++ {
	}

	return i > 0
}

const checkCreatedBySubstr = "created by "

func checkCreatedBy(s string) bool {
	i := strings.Index(s, checkCreatedBySubstr)
	if i == -1 {
		return false
	}

	s = s[i+len(checkCreatedBySubstr):]

	return strings.IndexByte(s, '.') != -1
}

func isIDChar(c byte) bool {
	isLowerCase := 'a' <= c && c <= 'z'
	isUpperCase := 'A' <= c && c <= 'Z'
	isDigit := '0' <= c && c <= '9'
	return isLowerCase || isUpperCase || isDigit || c == '_'
}

func isIDCharNoDigit(c byte) bool {
	isLowerCase := 'a' <= c && c <= 'z'
	isUpperCase := 'A' <= c && c <= 'Z'
	return isLowerCase || isUpperCase || c == '_'
}

func checkMethodCallIndex(s string, pos int) bool {
	if s[pos] != ')' {
		return false
	}

	i := strings.LastIndex(s[:pos], "(")
	if i == -1 {
		return false
	}

	right := i - 1
	left := right
	for ; left >= 0 && isIDChar(s[left]); left-- {
	}

	if left == right {
		return false
	}

	if left == -1 {
		return false
	}

	if s[left] != '.' {
		return false
	}

	left--
	if left >= 0 && s[left] == ')' {
		left--
	}

	return checkEndsWithClassName(s[:left])
}

func checkEndsWithClassName(s string) bool {
	i := len(s) - 1
	for ; i >= 0; i-- {
		c := s[i]
		if isIDCharNoDigit(c) {
			break
		}

		isDigit := '0' <= c && c <= '9'
		if !isDigit {
			return false
		}
	}

	return i >= 0
}

func checkMethodCall(s string) bool {
	for i := len(s) - 1; i >= 0; i-- {
		res := checkMethodCallIndex(s, i)
		if res {
			return true
		}
	}

	return false
}

const (
	checkPanicPrefix1 = "panic"
	checkPanicPrefix2 = "0x"
)

func checkPanicAddress(s string) bool {
	i := strings.Index(s, checkPanicPrefix1)
	if i == -1 {
		return false
	}

	s = s[i+len(checkPanicPrefix1):]

	i = strings.Index(s, checkPanicPrefix2)
	if i == -1 {
		return false
	}

	// no chars between parts
	if i == 0 {
		return false
	}

	s = s[i+len(checkPanicPrefix2):]

	if s == "" {
		return false
	}

	c := s[0]
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f'
}
