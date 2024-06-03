package metadata

import (
	"bytes"
	"fmt"
	"regexp"
	"sync"
	"text/template"

	"github.com/ozontech/file.d/cfg"
)

type MetaData map[string]string

type MetaTemplater struct {
	templates    map[string]*template.Template
	singleValues map[string]string
	poolBuffer   sync.Pool
}

func NewMetaTemplater(templates cfg.MetaTemplates) *MetaTemplater {
	compiledTemplates := make(map[string]*template.Template)
	singleValues := make(map[string]string)
	singleValueRegex := regexp.MustCompile(`^\{\{\ +\.(\w+)\ +\}\}$`)

	for k, v := range templates {
		vals := singleValueRegex.FindStringSubmatch(v)
		if len(vals) > 1 {
			singleValues[k] = vals[1]
		} else {
			compiledTemplates[k] = template.Must(template.New("").Parse(v))
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
	values := data.GetData()
	meta := MetaData{}

	if len(m.templates) > 0 {
		tplOutput := m.poolBuffer.Get().(*bytes.Buffer)
		defer m.poolBuffer.Put(tplOutput)

		for k, tmpl := range m.templates {
			tplOutput.Reset()
			err := tmpl.Execute(tplOutput, values)
			if err != nil {
				return meta, err
			} else {
				meta[k] = tplOutput.String()
			}
		}
	}

	for k, tmpl := range m.singleValues {
		if val, ok := values[tmpl]; ok {
			meta[k] = fmt.Sprintf("%v", val)
		}
	}

	return meta, nil
}
