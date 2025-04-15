package template

import (
	"fmt"
)

const (
	nameGoPanic     = "go_panic"
	nameCSException = "cs_exception"
	nameGoDataRace  = "go_data_race"
)

type joinTemplates map[string]struct {
	startCheckFunc    func(string) bool
	continueCheckFunc func(string) bool
	negate            bool
}

var templates = joinTemplates{
	// start regex: ^(panic:)|(http: panic serving)|^(fatal error:)
	// continue regex: (^\s*$)|(goroutine [0-9]+ \[)|(\.go:[0-9]+)|(created by .*\/?.*\.)|(^\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)|([A-Za-z_]+[A-Za-z0-9_]*\)?\.[A-Za-z0-9_]+\(.*\))
	nameGoPanic: {
		startCheckFunc:    goPanicStartCheck,
		continueCheckFunc: goPanicContinueCheck,
	},

	// start regex: ^\s*(?i)Unhandled exception
	// continue regex: (^\s*at\s.*)|(\s*--->)|(^(?i)\s*--- End of)|(\.?\w+\.?Exception:)
	nameCSException: {
		startCheckFunc:    sharpStartCheck,
		continueCheckFunc: sharpContinueCheck,
	},
	nameGoDataRace: {
		startCheckFunc:    goDataRaceStartCheck,
		continueCheckFunc: goDataRaceFinishCheck,
		negate:            true,
	},
}

type Template struct {
	StartCheck    func(string) bool
	ContinueCheck func(string) bool
	Negate        bool
}

func InitTemplate(name string) (Template, error) {
	cur, ok := templates[name]
	if !ok {
		return Template{}, fmt.Errorf("join template \"%s\" not found", name)
	}

	if cur.startCheckFunc == nil || cur.continueCheckFunc == nil {
		return Template{}, fmt.Errorf("no fast check funcs for template \"%s\"", name)
	}

	return Template{
		StartCheck:    cur.startCheckFunc,
		ContinueCheck: cur.continueCheckFunc,
		Negate:        cur.negate,
	}, nil
}
