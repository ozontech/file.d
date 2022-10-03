package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLogFilename2(t *testing.T) {
	pod, container, uniq := parseLogFilename("/var/log/pods/bx_cdp-data-manager-release-cdp-5396-cron-re-tasks-27748185-hgl65_64f4808f-377a-423f-b47b-8f0d89da6f2f/xds-init-container/0.log")
	assert.Equal(t, "bx_cdp", pod)
	assert.Equal(t, "xds-init-container", container)
	assert.Equal(t, "bx_cdp-data-manager-release-cdp-5396-cron-re-tasks-27748185-hgl65_64f4808f-377a-423f-b47b-8f0d89da6f2f", uniq)
}
