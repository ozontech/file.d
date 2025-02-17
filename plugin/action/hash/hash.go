package hash

import (
	"github.com/cespare/xxhash/v2"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/hash/normalize"
	insaneJSON "github.com/ozontech/insane-json"
)

/*{ introduction
It calculates the hash for one of the specified event fields and adds a new field with result in the event root.
> Fields can be of any type except for an object and an array.
}*/

/*{ examples
Hashing without normalization (first found field is `error.code`):
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: error.code
        - field: level
      result_field: hash
    ...
```
The original event:
```json
{
  "level": "error",
  "error": {
    "code": "unauthenticated",
    "message": "bad token format"
  }
}
```
The resulting event:
```json
{
  "level": "error",
  "error": {
    "code": "unauthenticated",
    "message": "bad token format"
  },
  "hash": 6584967863753642363,
}
```
---
Hashing with normalization (first found field is `message`):
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: error.code
        - field: message
          format: normalize
      result_field: hash
    ...
```
The original event:
```json
{
  "level": "error",
  "message": "2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\""
}
```

Normalized 'message':
`<datetime> error occurred, client: <ip>, upstream: "<url>", host: "<host>:<int>"`

The resulting event:
```json
{
  "level": "error",
  "message": "2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\"",
  "hash": 13863947727397728753,
}
```
}*/

type Plugin struct {
	config *Config

	normalizer normalize.Normalizer
	buf        []byte
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Prioritized list of fields. The first field found will be used to calculate the hash.
	// >
	// > `Field` params:
	// > * **`field`** *`cfg.FieldSelector`* *`required`*
	// >
	// > 	The event field for calculating the hash.
	// >
	// > * **`format`** *`string`* *`default=no`* *`options=no|normalize`*
	// >
	// > 	The field format for various hashing algorithms.
	// >
	// > * **`max_size`** *`int`* *`default=0`*
	// >
	// > 	The maximum field size used in hash calculation of any format.
	// > 	If set to `0`, the entire field will be used in hash calculation.
	// >
	// > 	> If the field size is greater than `max_size`, then
	// > 	the first `max_size` bytes will be used in hash calculation.
	// >	>
	// > 	> It can be useful in case of performance degradation when calculating the hash of long fields.
	Fields []Field `json:"fields" slice:"true" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The event field to which put the hash.
	ResultField  cfg.FieldSelector `json:"result_field" parse:"selector" required:"true"` // *
	ResultField_ []string
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

	MaxSize int `json:"max_size" default:"0"`
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

	p.buf = make([]byte, 0)

	for _, f := range p.config.Fields {
		if f.Format_ == ffNormalize {
			p.normalizer = normalize.NewTokenNormalizer()
			break
		}
	}
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	var (
		fieldNode *insaneJSON.Node
		field     Field
	)
	for _, f := range p.config.Fields {
		fieldNode = event.Root.Dig(f.Field_...)
		if fieldNode != nil && !(fieldNode.IsArray() || fieldNode.IsObject()) {
			field = f
			break
		}
		fieldNode = nil
	}
	if fieldNode == nil {
		return pipeline.ActionPass
	}

	fieldData := fieldNode.AsBytes()
	hashSize := len(fieldData)
	if field.MaxSize > 0 && hashSize > field.MaxSize {
		hashSize = field.MaxSize
	}

	var hash uint64
	switch field.Format_ {
	case ffNo:
		hash = calcHash(fieldData[:hashSize])
	case ffNormalize:
		hash = calcHash(p.normalizer.Normalize(p.buf, fieldData[:hashSize]))
	}

	pipeline.CreateNestedField(event.Root, p.config.ResultField_).MutateToUint64(hash)
	return pipeline.ActionPass
}

func calcHash(data []byte) uint64 {
	return xxhash.Sum64(data)
}
