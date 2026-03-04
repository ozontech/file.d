package antispam

import "github.com/ozontech/file.d/pipeline/doif"

type Rule struct {
	Name        string        `json:"name"`
	Threshold   int           `json:"threshold"`
	DoIfChecker *doif.Checker `json:"do_if"`
}

type Rules []Rule

type antispamData struct {
	eventBytes []byte
	sourceName string
	meta       map[string]string
}

func (d *antispamData) Get(args ...string) []byte {
	if len(args) == 0 {
		return nil
	}
	switch args[0] {
	case "event":
		return d.eventBytes
	case "source":
		return []byte(d.sourceName)
	case "meta":
		if len(args) != 2 {
			return nil
		}
		if v, ok := d.meta[args[1]]; ok {
			return []byte(v)
		}
	}
	return nil
}
