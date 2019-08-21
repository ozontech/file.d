package pipeline

import (
	"github.com/valyala/fastjson"
)

type Event struct {
	parser *fastjson.Parser
	json   *fastjson.Value

	input    InputPlugin
	Offset   int64
	SourceId uint64
	Stream   string

	index int
	next  *Event

	// some debugging shit
	raw []byte
}
