package test

import (
	"fmt"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/plugin/action/throttle"
	filein "github.com/ozontech/file.d/plugin/input/file"
	"github.com/ozontech/file.d/plugin/input/http"
	"github.com/ozontech/file.d/plugin/input/k8s"
	fileout "github.com/ozontech/file.d/plugin/output/file"
	"github.com/ozontech/file.d/plugin/output/s3"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHierarchicalConfigs(t *testing.T) {
	t.Skip()

	{
		fmt.Println("throttle")
		s := &throttle.Config{
			TimeFieldFormat: "unixdate",
			LimitKind:       "size",
			LimiterBackend:  "redis",
		}
		err := cfg.Parse(s, map[string]int{})
		require.NoError(t, err, "shouldn't be an error")
	}

	{
		fmt.Println("http")
		s := &http.Config{
			EmulateMode: "no",
			Auth: http.AuthConfig{
				Strategy: "disabled",
			},
		}
		err := cfg.Parse(s, map[string]int{})
		require.NoError(t, err, "shouldn't be an error")
	}

	{
		fmt.Println("k8s")
		s := &k8s.Config{
			OffsetsFile: "qwe",
			WatchingDir: "qwe",
			FileConfig: filein.Config{
				PersistenceMode: "async",
				OffsetsOp:       "continue",
				WorkersCount:    "1",
			},
		}
		err := cfg.Parse(s, map[string]int{})
		require.NoError(t, err, "shouldn't be an error")
	}

	{
		fmt.Println("s3")
		s := &s3.Config{
			FileConfig: fileout.Config{
				TargetFile:         "qwe",
				RetentionInterval:  "1s",
				RetentionInterval_: 0,
				Layout:             "qwe",
				WorkersCount:       "123",
				WorkersCount_:      0,
				BatchSize:          "123",
				BatchSize_:         0,
				BatchSizeBytes:     "123",
				BatchSizeBytes_:    0,
				BatchFlushTimeout:  "123s",
				BatchFlushTimeout_: 0,
				FileMode:           "123",
				FileMode_:          0,
			},
			CompressionType: "zip",
			Endpoint:        "qwe",
			AccessKey:       "qwe",
			SecretKey:       "qwe",
			DefaultBucket:   "qwe",
		}
		err := cfg.Parse(s, map[string]int{})
		require.NoError(t, err, "shouldn't be an error")
	}
}
