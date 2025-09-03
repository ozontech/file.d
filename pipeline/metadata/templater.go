package metadata

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"text/template"

	"github.com/dominikbraun/graph"
	"github.com/elliotchance/orderedmap/v2"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ozontech/file.d/cfg"
	"go.uber.org/zap"
)

type MetaData map[string]string

type ValueType string

const (
	// when template for a sinlge value (e.g., "{{ .key }}")
	SingleValueType ValueType = "single"

	// when template is complex and we have to use template.Execute (e.g., "value_{{ .key }}")
	TemplateValueType ValueType = "template"
)

// MetaTemplate holds the template and its original string
type MetaTemplate struct {
	Template *template.Template
	Source   string
}

// NewMetaTemplate creates a new TemplateWrapper with default function
func NewMetaTemplate(source string) *MetaTemplate {
	tmpl := template.Must(template.New("").Funcs(template.FuncMap{
		"default": func(defaultValue string, value interface{}) interface{} {
			if value == nil || value == "" {
				return defaultValue
			}
			return value
		},
	}).Parse(source))
	return &MetaTemplate{Template: tmpl, Source: source}
}

type MetaTemplater struct {
	templates    map[string]*MetaTemplate
	singleValues map[string]string
	valueTypes   *orderedmap.OrderedMap[string, ValueType]
	poolBuffer   sync.Pool
	logger       *zap.Logger
	cache        *lru.Cache[string, MetaData]
}

func NewMetaTemplater(templates cfg.MetaTemplates, logger *zap.Logger, cacheSize int) *MetaTemplater {
	// Regular expression to find ALL keys in the template strings (e.g., {{ .key }})
	re := regexp.MustCompile(`{{\s*([^}]+)\s*}}`)

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
			expression := strings.TrimSpace(match[1])
			components := strings.Fields(expression)
			for _, component := range components {
				// catch all variables
				if !strings.HasPrefix(component, ".") {
					continue
				}

				parts := strings.Split(component, ".")
				if len(parts) == 0 {
					continue
				}

				// extract top-nested variable (e.g., .headers.sub_header.sub_sub_header => .headers)
				topNestedVariable := parts[1]
				if _, exists := templates[topNestedVariable]; !exists {
					continue
				}

				if _, err := g.Vertex(topNestedVariable); err != nil {
					// The key vertex has not been added before
					_ = g.AddVertex(topNestedVariable)
				}
				// for variable name we need get topNestedVariable
				_ = g.AddEdge(topNestedVariable, name)
			}
		}
	}

	// Topological sort on the graph to determine the order of template processing
	orderedParams, _ := graph.TopologicalSort(g)

	compiledTemplates := make(map[string]*MetaTemplate)
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
			compiledTemplates[k] = NewMetaTemplate(v)
			valueTypes.Set(k, TemplateValueType)
		}
	}

	cache, err := lru.New[string, MetaData](cacheSize)
	if err != nil {
		panic(err)
	}

	meta := MetaTemplater{
		templates:    compiledTemplates,
		singleValues: singleValues,
		valueTypes:   valueTypes,
		logger:       logger,
		poolBuffer: sync.Pool{
			New: func() interface{} { return new(bytes.Buffer) },
		},
		cache: cache,
	}

	return &meta
}

type Data interface {
	GetData() map[string]any
}

func (m *MetaTemplater) Render(data Data) (MetaData, error) {
	initValues := data.GetData()
	meta := MetaData{}

	// Create a unique cache key based on the input data
	cacheKey := generateCacheKey(initValues)

	// Check if the result is already cached
	if cachedMeta, found := m.cache.Get(cacheKey); found {
		return cachedMeta, nil
	}

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
			} else {
				m.logger.Error(
					fmt.Sprintf("cannot render meta field %s: no value {{ .%s }}", k, tmpl),
				)
			}
		} else if v == TemplateValueType {
			tmpl := m.templates[k]
			tplOutput.Reset()
			err := tmpl.Template.Execute(tplOutput, values)
			if err != nil {
				m.logger.Error(
					fmt.Sprintf("cannot render meta field %s", k),
					zap.String("template", tmpl.Source),
					zap.Error(err),
				)
				meta[k] = err.Error()
			} else {
				meta[k] = tplOutput.String()
				values[k] = meta[k]
			}
		}
	}

	m.cache.Add(cacheKey, meta)

	return meta, nil
}

func generateCacheKey(data map[string]any) string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var builder strings.Builder
	builder.Grow(len(data) * 32) // Preallocate memory for the builder (estimate)

	for k, v := range data {
		switch v := v.(type) {
		case string:
			// Write the key and string value to the builder
			builder.WriteString(k)
			builder.WriteString(":")
			builder.WriteString(v)
			builder.WriteString("|")
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			builder.WriteString(k)
			builder.WriteString(":")
			builder.WriteString(fmt.Sprintf("%d", v))
			builder.WriteString("|")
		case float32, float64:
			builder.WriteString(k)
			builder.WriteString(":")
			builder.WriteString(fmt.Sprintf("%f", v))
			builder.WriteString("|")
		case bool:
			builder.WriteString(k)
			builder.WriteString(":")
			builder.WriteString(fmt.Sprintf("%t", v))
			builder.WriteString("|")
		}
		// If the value is not a string, int, float or bool, skip it
	}

	// Convert the builder to a string
	key := builder.String()

	// Remove the last "|" character if needed
	if key != "" {
		key = key[:len(key)-1] // Slice to remove the last character
	}

	return key
}
