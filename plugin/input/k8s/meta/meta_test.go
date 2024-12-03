package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	DisableMetaUpdates = true
	m.Run()
}

func TestParseLogFilename(t *testing.T) {
	metaInfo, err := NewK8sMetaInformation("/k8s-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log")

	assert.Nil(t, err)
	assert.Equal(t, "sre", metaInfo.Namespace)
	assert.Equal(t, "advanced-logs-checker-1566485760-trtrq", metaInfo.PodName)
	assert.Equal(t, "duty-bot", metaInfo.ContainerName)
	assert.Equal(t, "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0", metaInfo.ContainerID)
}

func TestParseLogFilenameError(t *testing.T) {
	_, err := NewK8sMetaInformation("web-logs-filed")

	assert.NotNil(t, err)
}
