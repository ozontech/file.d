package file

import (
	"fmt"
	"net/http"

	"github.com/ozontech/file.d/logger"
)

var InfoRegistryInstance = &InfoRegistry{}

type InfoRegistry struct {
	plug *Plugin
}

func (ir *InfoRegistry) AddPlugin(plug *Plugin) {
	ir.plug = plug
}

func (ir *InfoRegistry) Info(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("<html><body><pre><p>"))

	jobsInfo := logger.Cond(len(ir.plug.jobProvider.jobs) == 0, logger.Header("no jobs"), func() string {
		o := logger.Header("jobs")
		ir.plug.jobProvider.jobsMu.RLock()
		for s, source := range ir.plug.jobProvider.jobs {
			o += fmt.Sprintf(
				"source_id: %d, filename: %s, inode: %d, offset: %d\n",
				s, source.filename,
				source.inode, source.curOffset,
			)
		}
		ir.plug.jobProvider.jobsMu.RUnlock()
		return o
	})

	_, _ = w.Write([]byte(jobsInfo))

	watcherInfo := logger.Header("watch_paths")
	for _, source := range ir.plug.jobProvider.watcher.basePaths {
		watcherInfo += fmt.Sprintf(
			"%s\n",
			source,
		)
	}
	watcherInfo += fmt.Sprintf(
		"commonPath: %s\n",
		ir.plug.jobProvider.watcher.commonPath,
	)
	_, _ = w.Write([]byte(watcherInfo))

	_, _ = w.Write([]byte("</p></pre></body></html>"))
}
