package metadata

import (
	"bytes"
	"fmt"
	"regexp"
	"sync"
	"text/template"

	"github.com/dominikbraun/graph"
	"github.com/elliotchance/orderedmap/v2"
	"github.com/ozontech/file.d/cfg"
)

type MetaData map[string]string

type MetaTemplater struct {
	templates    *orderedmap.OrderedMap[string, *template.Template]
	singleValues *orderedmap.OrderedMap[string, string]
	poolBuffer   sync.Pool
}

func NewMetaTemplater(templates cfg.MetaTemplates) *MetaTemplater {
	// Regular expression to find keys in the template strings
	re := regexp.MustCompile(`{{\s*\.(\w+)\s*}}`)
	g := graph.New(graph.StringHash, graph.Directed(), graph.PreventCycles())

	// Build a dependency graph
	for name, template := range templates {
		matches := re.FindAllStringSubmatch(template, -1)
		_ = g.AddVertex(name)
		for _, match := range matches {
			if len(match) <= 1 {
				continue
			}
			key := match[1]
			if _, exists := templates[key]; !exists {
				continue
			}
			if _, err := g.Vertex(key); err != nil {
				// The key vertex has not been added before
				_ = g.AddVertex(key)
			}
			_ = g.AddEdge(key, name)
		}
	}
	orderedParams, _ := graph.TopologicalSort(g)

	compiledTemplates := orderedmap.NewOrderedMap[string, *template.Template]()
	singleValues := orderedmap.NewOrderedMap[string, string]()
	singleValueRegex := regexp.MustCompile(`^\{\{\ +\.(\w+)\ +\}\}$`)

	for i := 0; i <= len(orderedParams)-1; i++ {
		k := orderedParams[i]
		v := templates[k]
		vals := singleValueRegex.FindStringSubmatch(v)
		if len(vals) > 1 {
			singleValues.Set(k, vals[1])
		} else {
			compiledTemplates.Set(k, template.Must(template.New("").Parse(v)))
		}
	}

	meta := MetaTemplater{
		templates:    compiledTemplates,
		singleValues: singleValues,
		poolBuffer: sync.Pool{
			New: func() interface{} { return new(bytes.Buffer) },
		},
	}

	return &meta
}

type Data interface {
	GetData() map[string]any
}

func (m *MetaTemplater) Render(data Data) (MetaData, error) {
	initValues := data.GetData()
	meta := MetaData{}
	values := make(map[string]any, len(initValues)+m.singleValues.Len())
	for key, value := range initValues {
		values[key] = value
	}

	for _, k := range m.singleValues.Keys() {
		tmpl, _ := m.singleValues.Get(k)
		if val, ok := values[tmpl]; ok {
			meta[k] = fmt.Sprintf("%v", val)
			values[k] = meta[k]
		}
	}

	if m.templates.Len() > 0 {
		tplOutput := m.poolBuffer.Get().(*bytes.Buffer)
		defer m.poolBuffer.Put(tplOutput)

		for _, k := range m.templates.Keys() {
			tmpl, _ := m.templates.Get(k)
			tplOutput.Reset()
			err := tmpl.Execute(tplOutput, values)
			if err != nil {
				return meta, err
			} else {
				meta[k] = tplOutput.String()
				values[k] = meta[k]
			}
		}
	}

	return meta, nil
}
