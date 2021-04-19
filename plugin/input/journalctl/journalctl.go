package journalctl

import (
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
)

/*{ introduction
Input plugin, that reads journalctl logs
}*/
type Plugin struct {
	params *pipeline.InputPluginParams
	config *Config
	reader *journalReader
	offset *offsetInfo
}

type Config struct {
	//! config-params
	//^ config-params

	//> @3@4@5@6
	//>
	//> The filename to store offsets of processed messages.
	OffsetsFile string `json:"offsets_file" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Additional args for journalctl
	//> Plugin forces "-o json" and "-c *cursor*" or "-n all", otherwise
	//> you can use almost any additional args
	//> https://man7.org/linux/man-pages/man1/journalctl.1.html
	JournalArgs []string `json:"journal_args" default:"['-f', '-a']"` //*

	// for testing mostly
	MaxLines int `json:"max_lines" default:"0"`
}

func (p *Plugin) Write(bytes []byte) (int, error) {
	p.params.Controller.In(0, "journalctl", p.offset.current, bytes, false)
	p.offset.current++
	return len(bytes), nil
}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "journalctl",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.params = params
	p.config = config.(*Config)

	p.offset = newOffsetInfo(p.config.OffsetsFile)
	if err := p.offset.openFile(); err != nil {
		p.params.Logger.Fatal(err)
	}
	p.offset.load(p.offset.file)

	readConfig := &journalReaderConfig{
		output:   p,
		cursor:   p.offset.cursor,
		maxLines: p.config.MaxLines,
		logger:   p.params.Logger,
	}
	p.reader = newJournalReader(readConfig)
	p.reader.args = append(p.reader.args, p.config.JournalArgs...)
	p.reader.start()
}

func (p *Plugin) Stop() {
	err := p.reader.stop()
	if err != nil {
		p.params.Logger.Error(err)
	}
	p.offset.clearFile()
	p.offset.save(p.offset.file)

	p.offset.closeFile()
}

func (p *Plugin) Commit(event *pipeline.Event) {
	p.offset.set(event.Root.Dig("__CURSOR").AsString())

	p.offset.clearFile()
	p.offset.save(p.offset.file)
}
