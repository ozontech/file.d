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

type ValueType string

const (
	// when template for a sinlge value (e.g., "{{ .key }}")
	SingleValueType ValueType = "single"

	// when template is complex and we have to use template.Execute (e.g., "value_{{ .key }}")
	TemplateValueType ValueType = "template"
)

type MetaTemplater struct {
	templates    map[string]*template.Template
	singleValues map[string]string
	valueTypes   *orderedmap.OrderedMap[string, ValueType]
	poolBuffer   sync.Pool
}

func NewMetaTemplater(templates cfg.MetaTemplates) *MetaTemplater {
	// Regular expression to find ALL keys in the template strings (e.g., {{ .key }})
	re := regexp.MustCompile(`{{\s*\.(\w+)\s*}}`)

	// Graph to manage dependencies between templates
	g := graph.New(graph.StringHash, graph.Directed(), graph.PreventCycles())

	// Build a dependency graph based on the templates
	for name, template := range templates {
		matches := re.FindAllStringSubmatch(template, -1)
		_ = g.AddVertex(name)

		// Iterate over all matches found in the template
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

	// Topological sort on the graph to determine the order of template processing
	orderedParams, _ := graph.TopologicalSort(g)

	compiledTemplates := make(map[string]*template.Template)
	singleValues := make(map[string]string)

	// Regular expression to match single value templates (e.g., {{ .key }})
	singleValueRegex := regexp.MustCompile(`^\{\{\ +\.(\w+)\ +\}\}$`)

	// Ordered map to keep track of value types (single or template)
	valueTypes := orderedmap.NewOrderedMap[string, ValueType]()

	for i := 0; i <= len(orderedParams)-1; i++ {
		k := orderedParams[i]
		v := templates[k]
		vals := singleValueRegex.FindStringSubmatch(v)
		if len(vals) > 1 {
			// "{{ .key }}" - signle value template
			singleValues[k] = vals[1]
			valueTypes.Set(k, SingleValueType)
		} else {
			// "value_{{ .key }}" - more complex template
			compiledTemplates[k] = template.Must(template.New("").Parse(v))
			valueTypes.Set(k, TemplateValueType)
		}
	}

	meta := MetaTemplater{
		templates:    compiledTemplates,
		singleValues: singleValues,
		valueTypes:   valueTypes,
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
	// for hold values
	values := make(map[string]any, len(initValues)+m.valueTypes.Len())
	for key, value := range initValues {
		values[key] = value
	}

	var tplOutput *bytes.Buffer
	if len(m.templates) > 0 {
		tplOutput = m.poolBuffer.Get().(*bytes.Buffer)
		defer m.poolBuffer.Put(tplOutput)
	}

	// Iterate over the keys in valueTypes to process each template or single value
	for _, k := range m.valueTypes.Keys() {
		v, _ := m.valueTypes.Get(k)
		if v == SingleValueType {
			tmpl := m.singleValues[k]
			if val, ok := values[tmpl]; ok {
				meta[k] = fmt.Sprintf("%v", val)
				values[k] = meta[k]
			}
		} else if v == TemplateValueType {
			tmpl := m.templates[k]
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
