package template

import (
	"fmt"
	"regexp"

	"github.com/ozontech/file.d/cfg"
)

const (
	nameGoPanic     = "go_panic"
	nameCSException = "cs_exception"
	nameGoDataRace  = "go_data_race"
)

type joinTemplates map[string]struct {
	startRePat    string
	continueRePat string

	startCheckFunc    func(string) bool
	continueCheckFunc func(string) bool

	negate bool
}

var templates = joinTemplates{
	nameGoPanic: {
		startRePat:    "/^(panic:)|(http: panic serving)|^(fatal error:)/",
		continueRePat: "/(^\\s*$)|(goroutine [0-9]+ \\[)|(\\.go:[0-9]+)|(created by .*\\/?.*\\.)|(^\\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)|([A-Za-z_]+[A-Za-z0-9_]*\\)?\\.[A-Za-z0-9_]+\\(.*\\))/",

		startCheckFunc:    goPanicStartCheck,
		continueCheckFunc: goPanicContinueCheck,
	},
	nameCSException: {
		startRePat:    `/^\s*(?i)Unhandled exception/`,
		continueRePat: `/(^\s*at\s.*)|(\s*--->)|(^(?i)\s*--- End of)|(\.?\w+\.?Exception:)/`,

		startCheckFunc:    sharpStartCheck,
		continueCheckFunc: sharpContinueCheck,
	},
	nameGoDataRace: {
		startRePat:    `/^WARNING: DATA RACE/`,
		continueRePat: `/^==================/`,

		startCheckFunc:    goDataRaceStartCheck,
		continueCheckFunc: goDataRaceFinishCheck,

		negate: true,
	},
}

type Template struct {
	StartRe       *regexp.Regexp
	ContinueRe    *regexp.Regexp
	StartCheck    func(string) bool
	ContinueCheck func(string) bool
	Negate        bool
}

func InitTemplate(name string) (Template, error) {
	cur, ok := templates[name]
	if !ok {
		return Template{}, fmt.Errorf("join template \"%s\" not found", name)
	}

	result := Template{
		StartCheck:    cur.startCheckFunc,
		ContinueCheck: cur.continueCheckFunc,
		Negate:        cur.negate,
	}

	var err error

	result.StartRe, err = cfg.CompileRegex(cur.startRePat)
	if err != nil {
		return Template{}, err
	}

	result.ContinueRe, err = cfg.CompileRegex(cur.continueRePat)
	if err != nil {
		return Template{}, err
	}

	return result, nil
}
