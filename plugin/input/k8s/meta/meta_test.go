package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLogFilename(t *testing.T) {
	metaInfo := NewK8sMetaInformation("/k8s-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log")

	assert.Equal(t, "sre", metaInfo.namespace)
	assert.Equal(t, "advanced-logs-checker-1566485760-trtrq", metaInfo.podName)
	assert.Equal(t, "duty-bot", metaInfo.containerName)
	assert.Equal(t, "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0", metaInfo.containerID)
}

func TestParseLogFilenameError(t *testing.T) {
	metaInfo := NewK8sMetaInformation("web-logs-filed")

	assert.Equal(t, "sre", metaInfo.namespace)
	assert.Equal(t, "advanced-logs-checker-1566485760-trtrq", metaInfo.podName)
	assert.Equal(t, "duty-bot", metaInfo.containerName)
	assert.Equal(t, "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0", metaInfo.containerID)
}
