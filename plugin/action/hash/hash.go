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
      hash_field: hash
    ...
```
The original event:
```json
{
  "level": "error",
  "error": {
    "code": "unauthenticated",
    "message": "bad token format"
  },
  "message": "2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\""
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
  "message": "2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\"",
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
      hash_field: hash
    ...
```
The original event:
```json
{
  "level": "error",
  "message": "2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\""
}
```
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
	Fields []Field `json:"fields" slice:"true" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The event field to which put the hash.
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

	p.normalizer = normalize.NewReNormalizer()
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
		fieldNode = nil
	}
	if fieldNode == nil {
		return pipeline.ActionPass
	}

	var hash uint64
	switch format {
	case ffNo:
		hash = calcHash(fieldNode.AsBytes())
	case ffNormalize:
		hash = calcHash(p.normalizer.Normalize(p.buf, fieldNode.AsBytes()))
	}

	pipeline.CreateNestedField(event.Root, p.config.HashField_).MutateToUint64(hash)
	return pipeline.ActionPass
}

func calcHash(data []byte) uint64 {
	return xxhash.Sum64(data)
}
