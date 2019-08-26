package pipeline

import (
	"github.com/valyala/fastjson"
)

type Event struct {
	parser *fastjson.Parser

	acceptor InputPluginAcceptor

	index int
	next  *Event

	// some debugging shit
	raw []byte

	JSON   *fastjson.Value
	Offset     int64
	SourceId   uint64
	Stream     string
	Additional string
}
