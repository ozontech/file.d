package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform"
	insaneJSON "github.com/ozontech/insane-json"
)

func main() {
	registry := transform.NewRegistry()
	registry.MustRegister(upcase{})

	var eventRaw string = `{"arr":["test0", "test1", "test2", "test3"]}`
	root := insaneJSON.Spawn()
	err := root.DecodeString(eventRaw)
	// node := root.Dig("arr", "2")

	// fmt.Println(node.AsString())

	source := `
		.level  = "info"
		ok = .status >= 200 && .status < 300
		if ok {
			.host = upcase(1)
		} else {
			abort
		}
	`

	source = `
		arr = ["one", 2, 3, "four", {"a": "A", "b": "B"}]
		.a = arr[4] 
		.a.b = "C"
		.a.c = {}
		.a.c.qwe = "qwe"
		.res = .a.a + .a["c"]["qwe"] + upcase(arr[3])
		.res.a = "test"
	 	# del .a
	`

	source = `
		arr = [{"key2":"obj1", "a":{"b":"B", "cc":{"c":"C"}}}]
		.res1 = arr[0]["key2"]
		.res2 = arr[0]["a"]["cc"]["c"]
	`

	prog, err := transform.Compile(source, registry)
	if err != nil {
		log.Fatal(err)
	}

	// events := []map[string]transform.Value{
	// 	{"host": transform.StringValue{V: "web-1"}, "status": transform.IntegerValue{V: 200}},
	// 	// {"host": transform.StringValue{V: "web-2"}, "status": transform.IntegerValue{V: 300}},
	// }

	// events := []*insaneJSON.Root{root}
	events := []*insaneJSON.Root{
		createEvent(`{"a":10, "b":3, "s":"hello"}`),
		// createEvent(`{"kind":"Event","arr":["test0", "test1", 2],"level":"Error"}`),
	}

	for _, event := range events {
		// out, aborted, err := prog.RunOnMap(event)
		// var out map[string]transform.Value
		// var aborted bool
		var err error

		// target := transform.NewMapTargetFrom(event)
		target := transform.NewRootTarget(event, "", nil)

		result, err := prog.Run(target)
		if err != nil {
			log.Printf("runtime error: %v", err)
			continue
		}

		// if result.Aborted {
		// 	out, aborted, err = nil, true, nil
		// } else {
		// 	out, aborted, err = target.Event(), false, nil
		// }

		if result.Aborted {
			fmt.Println("(aborted)")
			continue
		}
		// fmt.Println(transform.ObjectValue{V: out})
		fmt.Println(target.Root.EncodeToString())
	}
}

func createEvent(eventRaw string) *insaneJSON.Root {
	root := insaneJSON.Spawn()
	root.DecodeString(eventRaw)
	return root
}

type upcase struct{}

func (upcase) Name() string { return "upcase" }

func (upcase) Params() []transform.Parameter {
	return []transform.Parameter{
		{
			Name:          "value",
			Required:      true,
			AcceptedKinds: []transform.ValueKind{transform.KindString},
		},
	}
}

func (upcase) Call(args map[string]transform.Value) (transform.Value, error) {
	val := args["value"].(transform.StringValue)
	return transform.StringValue{V: strings.ToUpper(val.V)}, nil
}
