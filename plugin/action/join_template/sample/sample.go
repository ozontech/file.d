package sample

import _ "embed"

//go:embed go_panic.txt
var Panics string

//go:embed go_panic_nil_nodes.txt
var PanicsWithNilNodes string

//go:embed cs_exception.txt
var SharpException string
