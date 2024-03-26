package pipeline

import (
	"bytes"
	"text/template"

	"github.com/ozontech/file.d/cfg"
)

type MetaData map[string]string

type MetaTemplater struct {
	templates map[string]*template.Template
}

func NewMetaTemplater(templates cfg.MetaTemplates) *MetaTemplater {
	compiledTemplates := make(map[string]*template.Template, len(templates))
	for k, v := range templates {
		compiledTemplates[k] = template.Must(template.New("").Parse(v))
	}

	meta := MetaTemplater{
		templates: compiledTemplates,
	}

	return &meta
}

type Data interface {
	GetData() map[string]interface{}
}

func (m *MetaTemplater) Render(data Data) (MetaData, error) {
	values := data.GetData()
	meta := MetaData{}
	for k, tmpl := range m.templates {
		var tplOutput bytes.Buffer
		err := tmpl.Execute(&tplOutput, values)
		if err != nil {
			return meta, err
		} else {
			meta[k] = tplOutput.String()
		}
	}
	return meta, nil
}
