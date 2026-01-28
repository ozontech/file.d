package hash

import (
	"errors"
	"fmt"

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
  "hash": 6584967863753642363
}
```
---
Hashing with `field.max_size`:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: message
          max_size: 10
      result_field: hash
    ...
```
The original event:
```json
{
  "level": "error",
  "message": "bad token format"
}
```

The part of the "message" field for which the hash will be calculated:
`bad token `

The resulting event:
```json
{
  "level": "error",
  "message": "bad token format",
  "hash": 6584967863753642363
}
```
---
Hashing with normalization (built-in patterns only):
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
  "message": "2023-10-30T13:35:33.638720813Z error occurred, client: 2001:db8::1, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\"",
  "hash": 13863947727397728753
}
```
---
Hashing with normalization (custom patterns only):
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: message
          format: normalize
      result_field: hash
      normalizer:
        builtin_patterns: "no"
        patterns:
          - placeholder: '<quoted_str>'
            re: '"[^"]*"'
            priority: 'first'
          - placeholder: '<date>'
            re: '\d\d.\d\d.\d\d\d\d'
            priority: 'first'
    ...
```
The original event:
```json
{
  "level": "error",
  "message": "request from \"ivanivanov\", signed on 19.03.2025"
}
```

Normalized "message":
`request from <quoted_str>, signed on <date>`

The resulting event:
```json
{
  "level": "error",
  "message": "request from \"ivanivanov\", signed on 19.03.2025",
  "hash": 6933347847764028189
}
```
---
Hashing with normalization (all built-in & custom patterns):
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: message
          format: normalize
      result_field: hash
      normalizer:
        builtin_patterns: "all"
        patterns:
          - placeholder: '<nginx_datetime>'
            re: '\d\d\d\d/\d\d/\d\d\ \d\d:\d\d:\d\d'
            priority: last
    ...
```
The original event:
```json
{
  "level": "error",
  "message": "2006/01/02 15:04:05 error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\""
}
```

Normalized "message":
`<nginx_datetime> error occurred, client: <ip>, upstream: <double_quoted>, host: <double_quoted>`

The resulting event:
```json
{
  "level": "error",
  "message": "2006/01/02 15:04:05 error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\"",
  "hash": 4150276598667727274
}
```
---
Hashing with normalization (partial built-in patterns):
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: message
          format: normalize
      result_field: hash
      normalizer:
        builtin_patterns: "square_bracketed|ip"
    ...
```
The original event:
```json
{
  "level": "error",
  "message": "2006/01/02 15:04:05 error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\", params: [param1, param2]"
}
```

Normalized "message":
`2006/01/02 15:04:05 error occurred, client: <ip>, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\", params: <square_bracketed>`

The resulting event:
```json
{
  "level": "error",
  "message": "2006/01/02 15:04:05 error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\", params: [param1, param2]",
  "hash": 15982987157336450215
}
```
}*/

var (
	// key - <pipeline name>_<action index>
	normalizerCache = map[string]normalize.Normalizer{}
)

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
	// > * **`builtin_patterns`** *`string`* *`default="all"`*
	// >
	// > 	List of [built-in patterns](/plugin/action/hash/normalize/README.md#built-in-patterns) (see `pattern id` column).
	// >
	// >	Format: `pattern_id1|pattern_id2|...|pattern_idN`.
	// >
	// >	Example: `host|url|square_bracketed`.
	// >
	// >	> * If set to `all` - all built-in patterns will be used.
	// >	> * If set to `no` - built-in patterns will not be used.
	// >
	// > * **`custom_patterns`** *`[]NormalizePattern`*
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
	// >		A priority of pattern. Works only if `normalizer.builtin_patterns != "no"`.
	// >
	// >		If set to `first`, pattern will be added before built-in, otherwise - after.
	// >
	// >		> If `normalizer.builtin_patterns = "no"`, then the priority is determined
	// >		by the order of the elements in `normalizer.custom_patterns`.
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
	BuiltinPatterns string             `json:"builtin_patterns" default:"all"`
	CustomPatterns  []NormalizePattern `json:"custom_patterns" slice:"true"`
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
	l := params.Logger.Desugar()

	p.buf = make([]byte, 0)

	// we use 1 normalizer for all processors with the
	// same pipeline name and the action-plugin index
	cacheKey := fmt.Sprintf("%s_%d", params.PipelineName, params.Index)

	for _, f := range p.config.Fields {
		if f.Format_ != ffNormalize {
			continue
		}

		if n, ok := normalizerCache[cacheKey]; ok {
			p.normalizer = n
			break
		}

		l.Info("create normalizer")
		n, err := p.initNormalizer()
		if err != nil {
			l.Fatal("can't create normalizer", zap.Error(err))
		}
		p.normalizer = n
		normalizerCache[cacheKey] = n
		break
	}
}

func (p *Plugin) initNormalizer() (normalize.Normalizer, error) {
	tnParams := normalize.TokenNormalizerParams{
		BuiltinPatterns: p.config.Normalizer.BuiltinPatterns,
		CustomPatterns:  make([]normalize.TokenPattern, 0, len(p.config.Normalizer.CustomPatterns)),
	}
	for _, p := range p.config.Normalizer.CustomPatterns {
		tnParams.CustomPatterns = append(tnParams.CustomPatterns, normalize.TokenPattern{
			Placeholder: p.Placeholder,
			RE:          p.RE,
			Priority:    p.Priority,
		})
	}

	n, err := normalize.NewTokenNormalizer(tnParams)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, errors.New("failed to compile lexer: bad patterns")
	}

	return n, nil
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
