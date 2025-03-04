package template

import "strings"

const (
	goDataRaceStartPrefix  = "WARNING: DATA RACE"
	goDataRaceFinishPrefix = "=================="
)

func goDataRaceStartCheck(s string) bool {
	return strings.HasPrefix(s, goDataRaceStartPrefix)
}

func goDataRaceFinishCheck(s string) bool {
	return strings.HasPrefix(s, goDataRaceFinishPrefix)
}
