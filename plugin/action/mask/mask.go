package mask

import (
	"regexp"
	"unicode/utf8"

	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
	insaneJSON "github.com/vitkovskii/insane-json"
)

/*{ introduction
Remask plugin matches event with regular expression, and substitution successfully matched symbols via template.
You can set many expressions.


**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      masks:
        - mask:
			re2: "\b\d{1,4}\D?\d{1,4}\D?\d{1,4}\D?\d{1,4}\b"
          	mask: "****-****-****-****"
    ...
```
There are three variants of math&replace methods:
* Call regex.Replace for every string in event (maskAll)
* First call regex.Match, if matching successful then call regex.Replace (maskIfMatched)
* First call regex.FindMatchingIndex, then replace finded symbol by index(maskByIndex)

}*/

const (
	substitution = byte('*')
)

type Plugin struct {
	config     *Config
	buf        []byte
	valueNodes []*insaneJSON.Node
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> List of Masks
	Masks []Mask `json:"masks"` //*
}

//! config-params
//^ config-params
type Mask struct {
	//> @3@4@5@6
	//>
	//> Regular expression used for masking
	ReStr string `json:"re2" default:"" required:"true"`

	//> @3@4@5@6
	//>
	//>
	// Substitution byte `json:"substitution" default:"*"`
	Groups []int `json:"groups"`

	re *regexp.Regexp
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "mask",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.buf = make([]byte, 0, params.PipelineSettings.AvgLogSize)
	p.valueNodes = make([]*insaneJSON.Node, 0)
	for i := range p.config.Masks {
		p.config.Masks[i].re = regexp.MustCompile(p.config.Masks[i].ReStr)
	}
}

func (p Plugin) Stop() {
}

func appendMask(src, dst []byte, begin, end int) (int, []byte) {
	runeCounter := utf8.RuneCount(src[begin:end])
	for j := 0; j < runeCounter; j++ {
		dst = append(dst, substitution)
	}
	offset := len(src[begin:end]) - runeCounter
	return offset, dst
}

func maskSection(src, dst []byte, begin, end int) (int, []byte) {
	dst = append(dst, src[0:begin]...)

	offset, dst := appendMask(src, dst, begin, end)

	if len(dst)+offset < len(src) {
		dst = append(dst, src[end:]...)
	}

	return offset, dst
}

func maskValue(value, buf []byte, re *regexp.Regexp, grp []int) []byte {
	buf = buf[:0]
	indexes := re.FindAllSubmatchIndex(value, -1)
	if len(indexes) == 0 {
		return buf
	}

	offset := 0
	for _, index := range indexes {
		for _, group := range grp {
			if group*2+1 >= len(index) {
				continue
			}
			offset, value = maskSection(
				value,
				buf,
				index[group*2]-offset,
				index[group*2+1]-offset,
			)
		}
	}

	return value
}

func getValueNodeList(currentNode *insaneJSON.Node, valueNodes []*insaneJSON.Node) []*insaneJSON.Node {
	switch {
	case currentNode.IsField():
		valueNodes = getValueNodeList(currentNode.AsFieldValue(), valueNodes)
	case currentNode.IsArray():
		for _, n := range currentNode.AsArray() {
			valueNodes = getValueNodeList(n, valueNodes)
		}
	case currentNode.IsObject():
		for _, n := range currentNode.AsFields() {
			valueNodes = getValueNodeList(n, valueNodes)
		}
	default:
		valueNodes = append(valueNodes, currentNode)
	}
	return valueNodes
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	root := event.Root.Node
	if root == nil {
		return pipeline.ActionPass
	}

	p.valueNodes = p.valueNodes[:0]
	p.valueNodes = getValueNodeList(root, p.valueNodes)

	for _, v := range p.valueNodes {
		value := v.AsBytes()
		isMasked := false
		for _, mask := range p.config.Masks {
			res := maskValue(value, p.buf, mask.re, mask.Groups)
			if len(res) > 0 {
				isMasked = true
				value = res
			}
		}
		if isMasked {
			v.MutateToBytes(value)
		}
	}

	return pipeline.ActionPass
}
