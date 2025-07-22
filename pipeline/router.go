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

}

func (r *Router) Fail(event *Event) {
	r.deadQueue.Out(event)
}

func (r *Router) Out(event *Event) {
	r.output.Out(event)
}

func (r *Router) Stop() {
	r.output.Stop()
}

func (r *Router) DeadQueueIsAvailable() bool {
	return r.deadQueue != nil
}

func (r *Router) Start(params *OutputPluginParams) {
	params.Router = *r
	r.output.Start(r.outputInfo.Config, params)
	r.deadQueue.Start(r.deadQueueInfo.Config, params)
}
