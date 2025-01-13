package file

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/ozontech/file.d/logger"
)

var InfoRegistryInstance = &InfoRegistry{
	plugins: make(map[string]*Plugin),
}

type InfoRegistry struct {
	plugins map[string]*Plugin
}

func (ir *InfoRegistry) AddPlugin(pipelineName string, plug *Plugin) {
	ir.plugins[pipelineName] = plug
}

func (ir *InfoRegistry) Info(w http.ResponseWriter, r *http.Request) {
	pipeline := strings.Split(r.URL.Path, "/")[2]
	plugin, ok := ir.plugins[pipeline]
	if !ok {
		logger.Panicf("pipeline '%s' is not registered", pipeline)
	}

	_, _ = w.Write([]byte("<html><body><pre><p>"))

	jobsInfo := logger.Cond(len(plugin.jobProvider.jobs) == 0, logger.Header("no jobs"), func() string {
		o := logger.Header("jobs")
		plugin.jobProvider.jobsMu.RLock()
		for s, source := range plugin.jobProvider.jobs {
			o += fmt.Sprintf(
				"source_id: %d, filename: %s, inode: %d, offset: %d\n",
				s, source.filename,
				source.inode, source.curOffset,
			)
		}
		plugin.jobProvider.jobsMu.RUnlock()
		return o
	})

	_, _ = w.Write([]byte(jobsInfo))

	watcherInfo := logger.Header("watch_paths")
	for _, source := range plugin.jobProvider.watcher.basePaths {
		watcherInfo += fmt.Sprintf(
			"%s\n",
			source,
		)
	}
	watcherInfo += fmt.Sprintf(
		"commonPath: %s\n",
		plugin.jobProvider.watcher.commonPath,
	)
	_, _ = w.Write([]byte(watcherInfo))

	_, _ = w.Write([]byte("</p></pre></body></html>"))
}
