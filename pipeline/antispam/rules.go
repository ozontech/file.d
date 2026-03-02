package antispam

import "github.com/ozontech/file.d/pipeline/doif"

type Rule struct {
	Name        string        `json:"name"`
	Threshold   int           `json:"threshold"`
	DoIfChecker *doif.Checker `json:"do_if"`
}

type Rules []Rule

type getAntispamData struct {
	eventBytes []byte
	sourceName string
	meta       map[string]string
}

func (g *getAntispamData) GetData(args ...string) []byte {
	if len(args) == 0 {
		panic("wrong usage")
	}
	switch args[0] {
	case "event":
		return g.eventBytes
	case "source":
		return []byte(g.sourceName)
	case "meta":
		if len(args) != 2 {
			panic("wrong usage")
		}
		if v, ok := g.meta[args[1]]; ok {
			return []byte(v)
		} else {
			panic("wrong")
		}
	default:
		panic("wrong")
	}
}
