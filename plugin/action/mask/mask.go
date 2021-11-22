package mask

import (
	"regexp"
	"unicode/utf8"

	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
Mask plugin matches event with regular expression and substitutions successfully matched symbols via asterix symbol.
You could set regular expressions and submatch groups.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      masks:
        - mask:
			re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
			groups: [1,2,3]
    ...
```

}*/

const substitution = byte('*')

type Plugin struct {
	config     *Config
	buf        []byte
	valueNodes []*insaneJSON.Node
	logger     *zap.SugaredLogger
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> List of masks
	Masks []Mask `json:"masks"` //*
}

type Mask struct {
	//> @3@4@5@6
	//>
	//> Regular expression for masking
	Re  string `json:"re" default:"" required:"true"` //*
	Re_ *regexp.Regexp

	//> @3@4@5@6
	//>
	// Numbers of masking groups in expression, zero for mask all expression
	Groups []int `json:"groups" required:"true"` //*
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
	p.logger = params.Logger
	for i := range p.config.Masks {
		p.logger.Infof("compiling re: %s, groups: %v", p.config.Masks[i].Re, p.config.Masks[i].Groups)
		p.config.Masks[i].Re_ = regexp.MustCompile(p.config.Masks[i].Re)
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

func maskValue(value, buf []byte, re *regexp.Regexp, groups []int, l *zap.SugaredLogger) []byte {
	buf = buf[:0]
	indexes := re.FindAllSubmatchIndex(value, -1)
	if len(indexes) == 0 {
		return buf
	}

	offset := 0
	for _, index := range indexes {
		for _, grp := range groups {
			maxIndexValue := grp*2 + 1
			if maxIndexValue < 0 || maxIndexValue >= len(index) || index[maxIndexValue] == -1 {
				l.Warnf("For re %s group %v doesn't exists", grp, re.String())
				continue
			}
			offset, value = maskSection(
				value,
				buf,
				index[grp*2]-offset,
				index[grp*2+1]-offset,
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

	p.valueNodes = p.valueNodes[:0]
	p.valueNodes = getValueNodeList(root, p.valueNodes)

	for _, v := range p.valueNodes {
		value := v.AsBytes()
		isMasked := false
		for _, mask := range p.config.Masks {
			res := maskValue(value, p.buf, mask.Re_, mask.Groups, p.logger)
			if len(res) > 0 {
				p.logger.Infof("value masked by re %s, groups %v", mask.Re, mask.Groups)
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
