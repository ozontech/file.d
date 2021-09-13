package file

import (
	"encoding/json"
	"net/http"
	"os"
	"sync"

	"github.com/ozonru/file.d/logger"
	"gopkg.in/yaml.v3"
)

// Resetter is a global struct for /reset endpoint of the plugin.
// It truncates jobs and tries to fix the offset file if the plugin can't load it.
type Resetter struct {
	plug     *Plugin
	offsetMu sync.Mutex
}

func (r *Resetter) setPlug(plug *Plugin) {
	r.plug = plug
}

// Reset truncates jobs if the plugin has started or delete the whole offset file
// or just one entry if inode or source_id was setted in a request.
func (r *Resetter) Reset(_ http.ResponseWriter, request *http.Request) {
	type Req struct {
		INode    uint64 `json:"inode,omitempty"`
		SourceID uint64 `json:"source_id,omitempty"`
	}
	var req Req
	dec := json.NewDecoder(request.Body)
	if err := dec.Decode(&req); err != nil {
		logger.Panicf("can't decode req body: %+v", request.Body)
	}

	if r.plug == nil {
		logger.Panicf("can't reset because plug has not been set")
	}

	if r.plug.jobProvider == nil {
		logger.Panicf("can't reset because file input plugin has not been started yet")
	}

	jp := r.plug.jobProvider

	truncateAll := req.INode == 0 && req.SourceID == 0

	if jp.isStarted {
		r.truncateJobs(truncateAll, req.INode, req.SourceID)

		return
	}

	r.offsetMu.Lock()
	defer r.offsetMu.Unlock()

	switch {
	case truncateAll:
		deleteOffsetFile(jp.offsetDB.curOffsetsFile)
	case req.INode > 0:
		deleteOneOffsetByField(jp.offsetDB, "inode", req.INode)
	case req.SourceID > 0:
		deleteOneOffsetByField(jp.offsetDB, "source_id", req.SourceID)
	}

	r.plug.params.Controller.RecoverFromPanic()
}

func (r *Resetter) truncateJobs(truncateAll bool, inode, sourceID uint64) {
	jp := r.plug.jobProvider

	jp.jobsMu.Lock()
	defer jp.jobsMu.Unlock()

	for _, j := range jp.jobs {
		if truncateAll || uint64(j.inode) == inode || uint64(j.sourceID) == sourceID {
			jp.truncateJob(j)
		}
	}
}

func deleteOffsetFile(f string) {
	if err := os.Remove(f); err != nil {
		logger.Panicf("can't remove file: %s", err)
	}
}

// deleteOneOffsetByField tries to parse the offset file and delete one entry by inode or source_id.
func deleteOneOffsetByField(o *offsetDB, fieldName string, fieldVal uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	f, err := os.ReadFile(o.curOffsetsFile)
	if err != nil {
		logger.Panicf("can't read file, try to reset all file. Error: %s", err.Error())
	}

	files := make([]map[string]interface{}, 0)
	err = yaml.Unmarshal(f, &files)
	if err != nil {
		logger.Panicf("can't unmarshal file, try to reset all file. err: %s file:\n%s", err.Error(), f)
	}

	for i := range files {
		field, ok := files[i][fieldName].(int)
		if !ok || field == 0 || uint64(field) != fieldVal {
			continue
		}

		last := len(files) - 1
		files[i] = files[last]
		files[last] = nil
		files = files[:last]

		out, err := yaml.Marshal(files)
		if err != nil {
			logger.Panicf("can't marshal file back, try to reset all file.")
		}

		err = os.WriteFile(o.curOffsetsFile, out, 0)
		if err != nil {
			logger.Panicf("can't write file back, try to reset all file.")
		}

		return
	}
}
