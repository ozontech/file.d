package pipeline

type router struct {
	output     OutputPlugin
	deadQueue  OutputPlugin
	outputInfo *OutputPluginInfo
}

func newRouter() *router {
	return &router{}
}

func (r *router) SetOutput(info *OutputPluginInfo) {
	r.outputInfo = info
	r.output = info.Plugin.(OutputPlugin)
}

func (r *router) SetDeadQueueOutput(info *OutputPluginInfo) {
	r.deadQueue = info.Plugin.(OutputPlugin)
}

func (p *router) Out(event *Event) {
	p.output.Out(event)
}

func (p *router) Stop() {
	p.output.Stop()
}

func (p *router) Start(config AnyConfig, params *OutputPluginParams) {
	p.output.Start(config, params)
}
