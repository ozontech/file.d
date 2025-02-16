package template

import (
	"regexp"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
)

const (
	goPanicStartRe    = "/^(panic:)|(http: panic serving)|^(fatal error:)/"
	goPanicContinueRe = "/(^\\s*$)|(goroutine [0-9]+ \\[)|(\\.go:[0-9]+)|(created by .*\\/?.*\\.)|(^\\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)|([A-Za-z_]+[A-Za-z0-9_]*\\)?\\.[A-Za-z0-9_]+\\(.*\\))/"

	sharpStartRe    = `/^\s*(?i)Unhandled exception/`
	sharpContinueRe = `/(^\s*at\s.*)|(\s*--->)|(^(?i)\s*--- End of)|(\.?\w+\.?Exception:)/`
)

var (
	GoPanicStartRe    *regexp.Regexp
	GoPanicContinueRe *regexp.Regexp

	SharpStartRe    *regexp.Regexp
	SharpContinueRe *regexp.Regexp
)

type pair struct {
	rePtr **regexp.Regexp
	src   string
}

func init() {
	var err error

	pairs := []pair{
		{&GoPanicStartRe, goPanicStartRe},
		{&GoPanicContinueRe, goPanicContinueRe},
		{&SharpStartRe, sharpStartRe},
		{&SharpContinueRe, sharpContinueRe},
	}

	for _, p := range pairs {
		*p.rePtr, err = cfg.CompileRegex(p.src)
		if err != nil {
			logger.Fatalf("failed to compile regex for template: %s", err.Error())
		}
	}
}
