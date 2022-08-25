package pipeline

import (
	"github.com/ozontech/file.d/decoder"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
)

func NewEmptyInputPluginController() InputPluginController {
	return &emptyInputPluginController{Ctl: metric.New("test_InputController")}
}

type emptyInputPluginController struct {
	*metric.Ctl
}

func (e emptyInputPluginController) In(sourceID SourceID, sourceName string, offset int64, data []byte, isNewSource bool) uint64 {
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
	logger.Error("used func IncMaxEventSizeExceeded, that not realized")
}

func NewEmptyOutputPluginController() OutputPluginController {
	return &emptyOutputPluginController{Ctl: metric.New("test_OutputController")}
}

type emptyOutputPluginController struct {
	*metric.Ctl
}

func (e *emptyOutputPluginController) Commit(event *Event) {
	logger.Error("used func Commit, that not realized")
}

func (e *emptyOutputPluginController) Error(err string) {
	logger.Error("used func Error, that not realized")
}

func NewActionOutputPluginController() ActionPluginController {
	return &emptyActionPluginController{Ctl: metric.New("test_OutputController")}
}

type emptyActionPluginController struct {
	*metric.Ctl
}

func (e emptyActionPluginController) Commit(event *Event) {
	logger.Error("used func Commit, that not realized")
}

func (e emptyActionPluginController) Propagate(event *Event) {
	logger.Error("used func Propagate, that not realized")
}
