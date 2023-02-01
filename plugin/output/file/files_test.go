package file

import (
	"testing"

	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestPluginsExists(t *testing.T) {
	cases := []struct {
		name        string
		prepareFunc func() *Plugins
		search      string
		expected    bool
	}{
		{
			name: "not_exists",
			prepareFunc: func() *Plugins {
				return NewFilePlugins(nil, metric.New("test", prometheus.NewRegistry()))
			},
			search:   "somePlugin",
			expected: false,
		},
		{
			name: "not_exists",
			prepareFunc: func() *Plugins {
				return NewFilePlugins(map[string]Plugable{
					"EXISTS": nil,
				}, metric.New("test", prometheus.NewRegistry()))
			},
			search:   "EXISTS",
			expected: true,
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			plugins := tCase.prepareFunc()
			exists := plugins.Exists(tCase.search)
			require.Equal(t, tCase.expected, exists)
		})
	}
}

func TestPluginsIsStatic(t *testing.T) {
	cases := []struct {
		name        string
		prepareFunc func() *Plugins
		search      string
		expected    bool
	}{
		{
			name: "not_static",
			prepareFunc: func() *Plugins {
				return NewFilePlugins(nil, metric.New("test", prometheus.NewRegistry()))
			},
			search:   "kekw",
			expected: false,
		},
		{
			name: "static",
			prepareFunc: func() *Plugins {
				return NewFilePlugins(map[string]Plugable{
					"plugname": nil,
				}, metric.New("test", prometheus.NewRegistry()))
			},
			search:   "plugname",
			expected: true,
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			plugins := tCase.prepareFunc()
			exists := plugins.IsStatic(tCase.search)
			require.Equal(t, tCase.expected, exists)
		})
	}
}

func TestPluginsIsDynamic(t *testing.T) {
	cases := []struct {
		name        string
		prepareFunc func() *Plugins
		search      string
		expected    bool
	}{
		{
			name: "not_dynamic",
			prepareFunc: func() *Plugins {
				return NewFilePlugins(nil, metric.New("test", prometheus.NewRegistry()))
			},
			search:   "kekw",
			expected: false,
		},
		{
			name: "dynamic",
			prepareFunc: func() *Plugins {
				plugins := NewFilePlugins(nil, metric.New("test", prometheus.NewRegistry()))
				plugins.dynamicPlugins["dynamic_plug"] = nil

				return plugins
			},
			search:   "dynamic_plug",
			expected: true,
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			plugins := tCase.prepareFunc()
			exists := plugins.IsDynamic(tCase.search)
			require.Equal(t, tCase.expected, exists)
		})
	}
}

func TestPluginsExistsIsStaticIsDynamic(t *testing.T) {
	staticPlug := "abc"
	dynamicPlug := "def"
	plugins := NewFilePlugins(map[string]Plugable{
		staticPlug: nil,
	}, metric.New("test", prometheus.NewRegistry()))
	plugins.Add(dynamicPlug, nil)

	require.True(t, plugins.Exists(staticPlug))
	require.True(t, plugins.IsStatic(staticPlug))
	require.False(t, plugins.IsDynamic(staticPlug))
	require.True(t, plugins.Exists(dynamicPlug))
	require.True(t, plugins.IsDynamic(dynamicPlug))
	require.False(t, plugins.IsStatic(dynamicPlug))

	imNotExist := "lol"
	require.False(t, plugins.Exists(imNotExist))
	require.False(t, plugins.IsDynamic(imNotExist))
	require.False(t, plugins.IsDynamic(imNotExist))
}
