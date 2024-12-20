package hash

import (
	"github.com/cespare/xxhash/v2"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
)

/*{ introduction
...
}*/

/*{ examples
...
}*/

type Plugin struct {
	config *Config

	normalizer normalizer
	buf        []byte
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > ...
	Fields []Field `json:"fields" slice:"true" required:"true"` // *

	// > @3@4@5@6
	// >
	// > ...
	HashField  cfg.FieldSelector `json:"hash_field" parse:"selector" required:"true"` // *
	HashField_ []string
}

type fieldFormat byte

const (
	ffNo fieldFormat = iota
	ffNormalize
)

type Field struct {
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"true"`
	Field_ []string

	Format  string `json:"format" default:"no" options:"no|normalize"`
	Format_ fieldFormat
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "hash",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	p.normalizer = newReNormalizer()
	p.buf = make([]byte, 0)
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	var (
		fieldNode *insaneJSON.Node
		format    fieldFormat
	)
	for _, f := range p.config.Fields {
		fieldNode = event.Root.Dig(f.Field_...)
		if fieldNode != nil && !(fieldNode.IsArray() || fieldNode.IsObject()) {
			format = f.Format_
			break
		}
	}
	if fieldNode == nil {
		return pipeline.ActionPass
	}

	var hash uint64
	switch format {
	case ffNo:
		hash = hashBytes(fieldNode.AsBytes())
	case ffNormalize:
		hash = hashBytes(p.normalizer.normalize(p.buf, fieldNode.AsBytes()))
	}

	pipeline.CreateNestedField(event.Root, p.config.HashField_).MutateToUint64(hash)
	return pipeline.ActionPass
}

func hashBytes(data []byte) uint64 {
	return xxhash.Sum64(data)
}
