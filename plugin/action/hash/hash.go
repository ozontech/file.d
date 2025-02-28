package hash

import (
	"github.com/cespare/xxhash/v2"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/hash/normalize"
	insaneJSON "github.com/ozontech/insane-json"
	"go.uber.org/zap"
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

Normalized "message":
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

	// > @3@4@5@6
	// >
	// > Normalizer params. It works for `fields` with `format: normalize`.
	// >> For more information, see [Normalization](/plugin/action/hash/normalize/README.md).
	// >
	// > `NormalizerConfig` params:
	// > * **`with_defaults`** *`bool`* *`default=true`*
	// >
	// > 	If set to `true`, normalizer will use `patterns` in combination with [default patterns](/plugin/action/hash/normalize/README.md#default-patterns).
	// >
	// > * **`patterns`** *`[]NormalizePattern`*
	// >
	// > 	List of normalization patterns.
	// >
	// > 	`NormalizePattern` params:
	// >	* **`placeholder`** *`string`* *`required`*
	// >
	// >		A placeholder that replaces the parts of string that falls under specified pattern.
	// >
	// >	* **`re`** *`string`* *`required`*
	// >
	// >		A regular expression that describes a pattern.
	// >		> We have some [limitations](/plugin/action/hash/normalize/README.md#limitations-of-the-re-language) of the RE syntax.
	// >
	// >	* **`priority`** *`string`* *`default=first`* *`options=first|last`*
	// >
	// >		A priority of pattern. Works only if `normalizer.with_defaults=true`.
	// >
	// >		If set to `first`, pattern will be added before defaults, otherwise - after.
	// >
	// >		> If `normalizer.with_defaults=false`, then the priority is determined
	// >		by the order of the elements in `normalizer.patterns`.
	Normalizer NormalizerConfig `json:"normalizer" child:"true"` // *
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

type NormalizePattern struct {
	Placeholder string `json:"placeholder" required:"true"`
	RE          string `json:"re" required:"true"`
	Priority    string `json:"priority" default:"first" options:"first|last"`
}

type NormalizerConfig struct {
	Patterns     []NormalizePattern `json:"patterns" slice:"true"`
	WithDefaults bool               `json:"with_defaults" default:"true"`
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
		if f.Format_ != ffNormalize {
			continue
		}

		if err := p.initNormalizer(); err != nil {
			params.Logger.Desugar().Fatal("can't create normalizer", zap.Error(err))
		}
		break
	}
}

func (p *Plugin) initNormalizer() error {
	tnParams := normalize.TokenNormalizerParams{
		WithDefaults: p.config.Normalizer.WithDefaults,
		Patterns:     make([]normalize.TokenPattern, 0, len(p.config.Normalizer.Patterns)),
	}
	for _, p := range p.config.Normalizer.Patterns {
		tnParams.Patterns = append(tnParams.Patterns, normalize.TokenPattern{
			Placeholder: p.Placeholder,
			RE:          p.RE,
			Priority:    p.Priority,
		})
	}

	n, err := normalize.NewTokenNormalizer(tnParams)
	if err != nil {
		return err
	}

	p.normalizer = n
	return nil
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
