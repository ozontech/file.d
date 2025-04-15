package template

import (
	"fmt"
)

const (
	nameGoPanic     = "go_panic"
	nameCSException = "cs_exception"
	nameGoDataRace  = "go_data_race"
)

type Template struct {
	StartCheck    func(string) bool
	ContinueCheck func(string) bool
	Negate        bool
}

var templates = map[string]Template{
	// start regex: ^(panic:)|(http: panic serving)|^(fatal error:)
	// continue regex: (^\s*$)|(goroutine [0-9]+ \[)|(\.go:[0-9]+)|(created by .*\/?.*\.)|(^\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)|([A-Za-z_]+[A-Za-z0-9_]*\)?\.[A-Za-z0-9_]+\(.*\))
	nameGoPanic: {
		StartCheck:    goPanicStartCheck,
		ContinueCheck: goPanicContinueCheck,
	},

	// start regex: ^\s*(?i)Unhandled exception
	// continue regex: (^\s*at\s.*)|(\s*--->)|(^(?i)\s*--- End of)|(\.?\w+\.?Exception:)
	nameCSException: {
		StartCheck:    sharpStartCheck,
		ContinueCheck: sharpContinueCheck,
	},
	nameGoDataRace: {
		StartCheck:    goDataRaceStartCheck,
		ContinueCheck: goDataRaceFinishCheck,
		Negate:        true,
	},
}

func InitTemplate(name string) (Template, error) {
	res, ok := templates[name]
	if !ok {
		return Template{}, fmt.Errorf("join template \"%s\" not found", name)
	}

	if res.StartCheck == nil || res.ContinueCheck == nil {
		return Template{}, fmt.Errorf("no check funcs for template \"%s\"", name)
	}

	return res, nil
}
