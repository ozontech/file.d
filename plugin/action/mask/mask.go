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

type Plugin struct {
	config *Config
	re     []*regexp.Regexp
	logger *zap.SugaredLogger
	buff   []byte
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
	Re string `json:"re2" default:"" required:"true"`

	//> @3@4@5@6
	//>
	//>	Substitution mask
	Substitution byte `json:"substitution" default:"*"`

	Groups []int `json:"groups"`
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
	if params != nil && params.Logger != nil {
		p.logger = params.Logger
	}
	p.config = config.(*Config)
	p.buff = make([]byte, 0, params.PipelineSettings.AvgLogSize)
	p.re = make([]*regexp.Regexp, len(p.config.Masks))
	for i, mask := range p.config.Masks {
		p.logger.Infof("compiling regexp %s/ - %s", mask.Re, mask.Substitution)
		p.re[i] = regexp.MustCompile(mask.Re)
	}
}

func (p Plugin) Stop() {
}

type section struct {
	begin, end int
}

// func find(slice []int, val int) bool {
// 	for _, item := range slice {
// 		if item == val {
// 			return true
// 		}
// 	}
// 	return false
// }

func transformMatchesToSections(input [][]int, hideGroups []int) []section {
	if len(hideGroups) == 0 {
		hideGroups = append(hideGroups, 0)
	}
	result := make([]section, 0, len(input))
	for _, matches := range input {
		for _, group := range hideGroups {
			if group*2+1 >= len(matches) {
				continue
			}
			result = append(result, section{matches[group*2], matches[group*2+1]})
		}
	}
	return result
}

func (p *Plugin) appendMaskToBuffer(s section, source []byte, ch byte) (offset int) {
	runeCounter := utf8.RuneCount(source[s.begin:s.end])
	for j := 0; j < runeCounter; j++ {
		if s.begin+j >= len(p.buff) {
			p.buff = append(p.buff, ch)
		} else {
			p.buff[s.begin+j] = ch
		}
	}
	offset = len(source[s.begin:s.end]) - runeCounter
	return
}

func (p *Plugin) maskSection(s section, source []byte, ch byte) int {
	if len(p.buff) < s.begin {
		p.buff = append(p.buff, source[len(p.buff):s.begin]...)
	}

	return p.appendMaskToBuffer(s, source, ch)
}

func (p *Plugin) mask(value []byte) ([]byte, bool) {
	isMasked := false

	for i, mask := range p.config.Masks {
		matches := p.re[i].FindAllSubmatchIndex(value, -1)
		if len(matches) == 0 {
			continue
		}
		p.buff = p.buff[:0]

		hideSections := transformMatchesToSections(matches, mask.Groups)

		offset := 0
		for _, section := range hideSections {
			offset += p.maskSection(section, value, mask.Substitution)
			isMasked = true
		}

		if len(p.buff)+offset < len(value) {
			p.buff = append(p.buff, value[len(p.buff)+offset:]...)
		}

		// if offset != 0 {
		value = p.buff[:]

		// }
	}

	if len(p.buff) == 0 {
		return value, false
	}

	return p.buff, isMasked
}

func collectValueNodes(currentNode *insaneJSON.Node, valueNodes *[]*insaneJSON.Node) {
	switch {
	case currentNode.IsField():
		collectValueNodes(currentNode.AsFieldValue(), valueNodes)
	case currentNode.IsArray():
		for _, n := range currentNode.AsArray() {
			collectValueNodes(n, valueNodes)
		}
	case currentNode.IsObject():
		for _, n := range currentNode.AsFields() {
			collectValueNodes(n, valueNodes)
		}
	default:
		*valueNodes = append(*valueNodes, currentNode)
	}
}

func getValueNodeList(rootNode *insaneJSON.Node) []*insaneJSON.Node {
	var nodes []*insaneJSON.Node
	collectValueNodes(rootNode, &nodes)
	return nodes
}

func (p Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	node := event.Root.Node
	if node == nil {
		return pipeline.ActionPass
	}

	nodes := getValueNodeList(node)

	for _, v := range nodes {
		data := v.AsBytes()
		res, isMasked := p.mask(data)
		if isMasked {
			v.MutateToBytes(res)
		}
	}

	return pipeline.ActionPass
}
