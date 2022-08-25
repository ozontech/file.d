package fake

import (
	"github.com/ozontech/file.d/decoder"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It provides an API to test pipelines and other plugins.
}*/

type Plugin struct {
	controller pipeline.InputPluginController
	commitFn   func(event *pipeline.Event)
	inFn       func()
}

type Config struct{}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "fake",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{controller: NewInputPluginController()}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.controller = params.Controller
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Commit(event *pipeline.Event) {
	if p.commitFn != nil {
		p.commitFn(event)
	}
}

// ! fn-list
// ^ fn-list

// > It sends a test event into the pipeline.
func (p *Plugin) In(sourceID pipeline.SourceID, sourceName string, offset int64, bytes []byte) { // *
	if p.inFn != nil {
		p.inFn()
	}
	_ = p.controller.In(sourceID, sourceName, offset, bytes, false)
}

// > It sets up a hook to make sure the test event has been successfully committed.
func (p *Plugin) SetCommitFn(fn func(event *pipeline.Event)) { // *
	p.commitFn = fn
}

// > It sets up a hook to make sure the test event has been passed to the plugin.
func (p *Plugin) SetInFn(fn func()) { // *
	p.inFn = fn
}

func NewInputPluginController() pipeline.InputPluginController {
	return &emptyInputPluginController{Ctl: metric.New("test_InputController")}
}

type emptyInputPluginController struct {
	*metric.Ctl
}

func (e emptyInputPluginController) In(sourceID pipeline.SourceID, sourceName string, offset int64, data []byte, isNewSource bool) uint64 {
	logger.Error("used func In, that not realized")
	return 0
}

func (e emptyInputPluginController) UseSpread() {
	logger.Error("used func UseSpread, that not realized")
}

func (e emptyInputPluginController) DisableStreams() {
	logger.Error("used func DisableStreams, that not realized")
}

func (e emptyInputPluginController) SuggestDecoder(t decoder.DecoderType) {
	logger.Error("used func SuggestDecoder, that not realized")
}

func (e emptyInputPluginController) IncReadOps() {
	logger.Error("used func IncReadOps, that not realized")
}

func (e emptyInputPluginController) IncMaxEventSizeExceeded() {
	//TODO implement me
	panic("implement me")
}
