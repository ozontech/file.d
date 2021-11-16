package s3

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go"
	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/plugin/input/fake"
	"github.com/ozonru/file.d/plugin/output/file"
	"github.com/ozonru/file.d/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

const (
	targetFile = "filetests/log.log"
)

var (
	dir, _ = filepath.Split(targetFile)

	fileName = ""
)

type mockClient struct{}

func newMockClient() objectStoreClient {
	return mockClient{}
}

func (m mockClient) BucketExists(bucketName string) (bool, error) {
	return true, nil
}

func (m mockClient) FPutObject(bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error) {
	fmt.Println("put object")
	targetDir := fmt.Sprintf("./%s", bucketName)
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		if err := os.MkdirAll(targetDir, os.ModePerm); err != nil {
			logger.Fatalf("could not create target dir: %s, error: %s", targetDir, err.Error())
		}
	}
	fileName = fmt.Sprintf("%s/%s", bucketName, "mockLog.txt")
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(0o777))
	if err != nil {
		logger.Panicf("could not open or create file: %s, error: %s", fileName, err.Error())
	}

	if _, err := file.WriteString(fmt.Sprintf("%s | from '%s' to b: `%s` as obj: `%s`\n", time.Now().String(), filePath, bucketName, objectName)); err != nil {
		return 0, fmt.Errorf(err.Error())
	}

	return 1, nil
}

func TestStart(t *testing.T) {
	tests := struct {
		firstPack  []test.Msg
		secondPack []test.Msg
		thirdPack  []test.Msg
	}{
		firstPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
		},
		secondPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
		},
		thirdPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
		},
	}

	file.FileSealUpInterval = 200 * time.Millisecond
	// pattern for parent log file
	pattern := fmt.Sprintf("%s/*.log", dir)

	writeFileSleep := 100*time.Millisecond + 100*time.Millisecond
	sealUpFileSleep := 2*file.FileSealUpInterval + 500*time.Millisecond
	test.ClearDir(t, dir)
	s3MockClient := newMockClient()
	fileConfig := file.Config{
		TargetFile:        targetFile,
		RetentionInterval: "300ms",
		Layout:            "01",
		BatchFlushTimeout: "100ms",
	}
	config := &Config{
		FileConfig:      fileConfig,
		CompressionType: "zip",
		Endpoint:        "some",
		AccessKey:       "some",
		SecretKey:       "some",
		Bucket:          "some",
		Secure:          false,
		client:          &s3MockClient,
	}
	test.ClearDir(t, dir)
	defer test.ClearDir(t, dir)
	test.ClearDir(t, fmt.Sprintf("%s/", config.Bucket))
	defer test.ClearDir(t, config.Bucket)
	err := cfg.Parse(config, map[string]int{"gomaxprocs": 1, "capacity": 64})
	assert.NoError(t, err)
	p := newPipeline(t, config)

	assert.NotNil(t, p, "could not create new pipeline")
	p.Start()
	time.Sleep(300 * time.Microsecond)

	test.SendPack(t, p, tests.firstPack)
	time.Sleep(writeFileSleep)
	time.Sleep(sealUpFileSleep)
	size1 := test.CheckNotZero(t, fileName, "s3 data is missed after first pack")

	// check deletion upload log files
	match := test.GetMatches(t, pattern)
	assert.Equal(t, 1, len(match))
	test.CheckZero(t, match[0], "log file is not nil")

	// initial sending the second pack
	// no special situations
	test.SendPack(t, p, tests.secondPack)
	time.Sleep(writeFileSleep)
	time.Sleep(sealUpFileSleep)

	match = test.GetMatches(t, pattern)
	assert.Equal(t, 1, len(match))
	test.CheckZero(t, match[0], "log file is not empty")

	size2 := test.CheckNotZero(t, fileName, "s3 data missed after second pack")
	assert.True(t, size2 > size1)

	// failed during writing
	test.SendPack(t, p, tests.thirdPack)
	time.Sleep(writeFileSleep - writeFileSleep/2)
	p.Stop()

	// check log file not empty
	match = test.GetMatches(t, pattern)
	assert.Equal(t, 1, len(match))
	test.CheckNotZero(t, match[0], "log file data missed")
	time.Sleep(sealUpFileSleep)

	// restart like after crash
	p.Start()

	time.Sleep(sealUpFileSleep / 2)

	size3 := test.CheckNotZero(t, fileName, "s3 data missed after third pack")
	assert.True(t, size3 > size2)
}

func newPipeline(t *testing.T, configOutput *Config) *pipeline.Pipeline {
	t.Helper()
	settings := &pipeline.Settings{
		Capacity:            4096,
		MaintenanceInterval: time.Second * 100000,
		AntispamThreshold:   0,
		AvgLogSize:          2048,
		StreamField:         "stream",
		Decoder:             "json",
	}

	http.DefaultServeMux = &http.ServeMux{}
	p := pipeline.New("test_pipeline", settings, prometheus.NewRegistry(), http.DefaultServeMux)
	p.DisableParallelism()
	p.EnableEventLog()

	anyPlugin, _ := fake.Factory()
	inputPlugin := anyPlugin.(*fake.Plugin)
	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type: "fake",
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: inputPlugin,
		},
	})

	// output plugin
	anyPlugin, _ = Factory()
	outputPlugin := anyPlugin.(*Plugin)
	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:   "s3",
			Config: configOutput,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: outputPlugin,
		},
	},
	)
	return p
}

func TestStartPanic(t *testing.T) {
	test.ClearDir(t, dir)
	fileConfig := file.Config{}
	config := &Config{
		FileConfig:      fileConfig,
		CompressionType: "zip",
		Endpoint:        "some",
		AccessKey:       "some",
		SecretKey:       "some",
		Bucket:          "some",
		Secure:          false,
		client:          nil,
	}
	test.ClearDir(t, dir)
	defer test.ClearDir(t, dir)
	test.ClearDir(t, config.Bucket)
	defer test.ClearDir(t, config.Bucket)

	err := cfg.Parse(config, map[string]int{"gomaxprocs": 1, "capacity": 64})
	assert.NoError(t, err)
	p := newPipeline(t, config)

	assert.NotNil(t, p, "could not create new pipeline")

	assert.Panics(t, p.Start)
}

type mockClientWIthSomeFails struct {
	ctx    context.Context
	Cancel context.CancelFunc
}

func newMockClientWIthSomeFails() objectStoreClient {
	m := mockClientWIthSomeFails{}
	ctx, cancel := context.WithCancel(context.Background())
	m.ctx = ctx
	m.Cancel = cancel
	return m
}

func (m mockClientWIthSomeFails) BucketExists(bucketName string) (bool, error) {
	return true, nil
}

func (m mockClientWIthSomeFails) FPutObject(bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error) {
	select {
	case <-m.ctx.Done():
		fmt.Println("put object")

		targetDir := fmt.Sprintf("./%s", bucketName)
		if _, err := os.Stat(targetDir); os.IsNotExist(err) {
			if err := os.MkdirAll(targetDir, os.ModePerm); err != nil {
				logger.Fatalf("could not create target dir: %s, error: %s", targetDir, err.Error())
			}
		}
		fileName = fmt.Sprintf("%s/%s", bucketName, "mockLog.txt")
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(0o777))
		if err != nil {
			logger.Panicf("could not open or create file: %s, error: %s", fileName, err.Error())
		}

		if _, err := file.WriteString(fmt.Sprintf("%s | from '%s' to b: `%s` as obj: `%s`\n", time.Now().String(), filePath, bucketName, objectName)); err != nil {
			return 0, fmt.Errorf(err.Error())
		}
		return 1, nil
	default:
		return 0, fmt.Errorf("fake could not sent")
	}
}

func TestStartWithSendProblems(t *testing.T) {
	tests := struct {
		firstPack  []test.Msg
		secondPack []test.Msg
		thirdPack  []test.Msg
	}{
		firstPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
		},
		secondPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
		},
		thirdPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
		},
	}

	file.FileSealUpInterval = 200 * time.Millisecond
	// pattern for parent log file
	pattern := fmt.Sprintf("%s/*.log", dir)
	zipPattern := fmt.Sprintf("%s/*.zip", dir)

	writeFileSleep := 100*time.Millisecond + 100*time.Millisecond
	sealUpFileSleep := 2*file.FileSealUpInterval + 500*time.Millisecond
	test.ClearDir(t, dir)
	s3MockClient := newMockClientWIthSomeFails()

	fileConfig := file.Config{
		TargetFile:        targetFile,
		RetentionInterval: "300ms",
		Layout:            "01",
		BatchFlushTimeout: "100ms",
	}
	config := &Config{
		FileConfig:      fileConfig,
		CompressionType: "zip",
		Endpoint:        "some",
		AccessKey:       "some",
		SecretKey:       "some",
		Bucket:          "some",
		Secure:          false,
		client:          &s3MockClient,
	}
	test.ClearDir(t, dir)
	defer test.ClearDir(t, dir)
	test.ClearDir(t, fmt.Sprintf("%s/", config.Bucket))
	defer test.ClearDir(t, config.Bucket)
	err := cfg.Parse(config, map[string]int{"gomaxprocs": 1, "capacity": 64})
	assert.NoError(t, err)
	p := newPipeline(t, config)

	assert.NotNil(t, p, "could not create new pipeline")

	p.Start()
	time.Sleep(300 * time.Microsecond)
	test.SendPack(t, p, tests.firstPack)
	time.Sleep(writeFileSleep)
	time.Sleep(sealUpFileSleep)

	noSentToS3(t)

	matches := test.GetMatches(t, zipPattern)

	assert.Equal(t, 1, len(matches))
	test.CheckNotZero(t, matches[0], "zip file after seal up and compress is not ok")

	matches = test.GetMatches(t, pattern)
	assert.Equal(t, 1, len(matches))
	test.CheckZero(t, matches[0], "log file is not empty")

	// initial sending the second pack
	// no special situations
	test.SendPack(t, p, tests.secondPack)
	time.Sleep(writeFileSleep)
	time.Sleep(sealUpFileSleep)

	matches = test.GetMatches(t, pattern)
	assert.Equal(t, 1, len(matches))
	test.CheckZero(t, matches[0], "log file is not empty")

	// check not empty zips
	matches = test.GetMatches(t, zipPattern)
	assert.GreaterOrEqual(t, len(matches), 2)
	for _, m := range matches {
		test.CheckNotZero(t, m, "zip file is empty")
	}

	noSentToS3(t)

	// allow sending to s3
	s3Client := s3MockClient.(mockClientWIthSomeFails)
	s3Client.Cancel()

	test.SendPack(t, p, tests.thirdPack)
	time.Sleep(writeFileSleep)
	time.Sleep(sealUpFileSleep)

	maxTimeWait := 5 * sealUpFileSleep
	sleep := sealUpFileSleep
	for {
		time.Sleep(sleep)
		// wait deletion
		matches = test.GetMatches(t, zipPattern)
		if len(matches) == 0 {
			logger.Infof("spent %f second of %f second of fake loading", sleep.Seconds(), maxTimeWait.Seconds())
			break
		}

		if sleep > maxTimeWait {
			logger.Infof("spent %f second of %f second of fake loading", sleep.Seconds(), maxTimeWait.Seconds())
			assert.Fail(t, "restart load is failed")
			break
		}
		sleep += sleep
	}

	// chek zip  in dir
	matches = test.GetMatches(t, zipPattern)
	assert.Equal(t, 0, len(matches))

	// check log file
	matches = test.GetMatches(t, pattern)
	assert.Equal(t, 1, len(matches))
	test.CheckZero(t, matches[0], "log file is not empty after restart sending")

	// check mock file is not empty and contains more than 3 raws
	test.CheckNotZero(t, fileName, "s3 file is empty")
	file, err := os.Open(fileName)
	assert.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCounter := 0
	for scanner.Scan() {
		fmt.Println(scanner.Text())
		lineCounter++
	}
	assert.GreaterOrEqual(t, lineCounter, 3)
}

func noSentToS3(t *testing.T) {
	t.Helper()
	// check no sent
	_, err := os.Stat(fileName)
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}
