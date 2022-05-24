package file

import (
	"sync"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

// Plugins is an abstraction upon multiple file.Plugin, which helps reuse it.
type Plugins struct {
	// plugins contains plugs that exist from start of work.
	plugins map[string]Plugable
	// dynamicPlugins contains plugs, that created dynamically during execution.
	// they separated from plugins to avoid races and reduce locking complexity.
	dynamicPlugins map[string]Plugable
	mu             sync.RWMutex
}

func NewFilePlugins(plugins map[string]Plugable) *Plugins {
	return &Plugins{plugins: plugins, dynamicPlugins: make(map[string]Plugable)}
}

func (p *Plugins) Out(event *pipeline.Event, selector pipeline.PluginSelector) {
	switch selector.CondType {
	case pipeline.ByNameSelector:
		p.out(event, selector)
	default:
		logger.Fatalf("PluginSelector type didn't set for event=%v, selector=%v", event, selector)
	}
}

func (p *Plugins) out(event *pipeline.Event, selector pipeline.PluginSelector) {
	if p.IsStatic(selector.CondValue) {
		p.plugins[selector.CondValue].Out(event)
	} else {
		p.mu.RLock()
		defer p.mu.RUnlock()

		p.dynamicPlugins[selector.CondValue].Out(event)
	}
}

// Start runs all plugins.
func (p *Plugins) Start(starterData pipeline.PluginsStarterMap) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for plugName, plug := range p.plugins {
		plug.Start(starterData[plugName].Config, starterData[plugName].Params)
	}
}

// Stop stops all plugins (very useful comment).
func (p *Plugins) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, plug := range p.plugins {
		plug.Stop()
	}
	for _, plug := range p.dynamicPlugins {
		plug.Stop()
	}
}

// Add new plugin to plugs.
func (p *Plugins) Add(plugName string, plug Plugable) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.dynamicPlugins[plugName] = plug
}

// Exists asks if such file.Plugin exists in Plugins.
// Concurrent safe.
func (p *Plugins) Exists(plugName string) (exists bool) {
	return p.IsStatic(plugName) || p.IsDynamic(plugName)
}

// IsStatic tells is plugin created from config.
func (p *Plugins) IsStatic(plugName string) bool {
	_, ok := p.plugins[plugName]
	return ok
}

// IsDynamic tells is plugin created from events.
func (p *Plugins) IsDynamic(PlugName string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	_, ok := p.dynamicPlugins[PlugName]
	return ok
}
