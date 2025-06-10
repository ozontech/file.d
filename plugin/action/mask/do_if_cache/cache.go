package do_if_cache

import (
	"fmt"

	"github.com/ozontech/file.d/pipeline/doif"
)

var cache = make(map[string][]*doif.Checker)

func key(pipelineName string, pluginIndex int) string {
	return fmt.Sprintf("%s_%d", pipelineName, pluginIndex)
}

func GetCheckers(pipelineName string, pluginIndex int) ([]*doif.Checker, bool) {
	res, ok := cache[key(pipelineName, pluginIndex)]
	return res, ok
}

func SetCheckers(pipelineName string, pluginIndex int, checkers []*doif.Checker) {
	cache[key(pipelineName, pluginIndex)] = checkers
}
