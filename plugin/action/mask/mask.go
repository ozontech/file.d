package mask

import (
	"regexp"

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
	Substitution string `json:"substitution" default:"" required:"true"`
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
	p.re = make([]*regexp.Regexp, len(p.config.Masks))
	for i, mask := range p.config.Masks {
		p.logger.Infof("compiling regexp %s/ - %s", mask.Re, mask.Substitution)
		p.re[i] = regexp.MustCompile(mask.Re)
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) maskByIndex(value string) (*string, bool) {
	isMasked := false
	offset := 0
	l := len(value)
	for i, mask := range p.config.Masks {
		matches := p.re[i].FindAllStringIndex(value, -1)
		for _, match := range matches {
			value = value[:match[0]+offset] + mask.Substitution + value[match[1]+offset:]
			isMasked = true
			offset = len(value) - l
			l = len(value)
			p.logger.Infof("mask by index %s", mask.Substitution)
		}
	}
	return &value, isMasked
}

func (p *Plugin) maskIfMatched(value string) (*string, bool) {
	isMasked := false
	for i, mask := range p.config.Masks {
		if p.re[i].MatchString(value) {
			isMasked = true
			value = p.re[i].ReplaceAllString(value, mask.Substitution)
			p.logger.Infof("mask %s", mask.Substitution)
		}
	}
	return &value, isMasked
}

func (p *Plugin) maskAll(value string) (*string, bool) {
	for i := range p.config.Masks {
		value = p.re[i].ReplaceAllString(value, p.config.Masks[i].Substitution)
	}
	return &value, true
}

func applyForStrings(node *insaneJSON.Node, applyFn func(string) (*string, bool)) {
	switch {
	case node.IsField():
		applyForStrings(node.AsFieldValue(), applyFn)
	case node.IsArray():
		for _, n := range node.AsArray() {
			applyForStrings(n, applyFn)
		}
	case node.IsObject():
		for _, n := range node.AsFields() {
			applyForStrings(n, applyFn)
		}
	default:
		if node.IsString() {
			str, isMasked := applyFn(node.AsString())
			if isMasked {
				node.MutateToString(*str)
			}
		}
	}
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	node := event.Root.Node
	if node == nil {
		return pipeline.ActionPass
	}
	applyForStrings(node, p.maskByIndex)
	return pipeline.ActionPass
}
