package pipeline

type Router struct {
	output     OutputPlugin
	outputInfo *OutputPluginInfo

	deadQueue     OutputPlugin
	deadQueueInfo *OutputPluginInfo
}

func NewRouter() *Router {
	return &Router{}
}

func (r *Router) SetOutput(info *OutputPluginInfo) {
	r.outputInfo = info
	r.output = info.Plugin.(OutputPlugin)
}

func (r *Router) SetDeadQueueOutput(info *OutputPluginInfo) {
	r.deadQueueInfo = info
	r.deadQueue = info.Plugin.(OutputPlugin)
}

func (r *Router) Ack(event *Event) {
	// TODO: send commit to input after receiving all acks from outputs
}

func (r *Router) Fail(event *Event) {
	if r.IsDeadQueueAvailable() {
		r.deadQueue.Out(event)
	}
}

func (r *Router) Out(event *Event) {
	r.output.Out(event)
}

func (r *Router) Stop() {
	r.output.Stop()
	if r.IsDeadQueueAvailable() {
		r.deadQueue.Stop()
	}
}

func (r *Router) IsDeadQueueAvailable() bool {
	return r.deadQueue != nil
}

func (r *Router) Start(params *OutputPluginParams) {
	params.Router = r
	r.output.Start(r.outputInfo.Config, params)
	if r.IsDeadQueueAvailable() {
		r.deadQueue.Start(r.deadQueueInfo.Config, params)
	}
}
