package s3

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/minio/minio-go"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/input/fake"
	"github.com/ozontech/file.d/plugin/output/file"
	mock_s3 "github.com/ozontech/file.d/plugin/output/s3/mock"
	"github.com/ozontech/file.d/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

const targetFile = "filetests/log.log"

var (
	dir, _   = filepath.Split(targetFile)
	fileName atomic.String
)

func testFactory(objStoreF objStoreFactory) (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &testS3Plugin{objStoreF: objStoreF}, &Config{}
}

type testS3Plugin struct {
	Plugin
	objStoreF objStoreFactory
}

func (p *testS3Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.Plugin.rnd = *rand.New(rand.NewSource(time.Now().UnixNano()))
	p.registerMetrics(params.MetricCtl)
	p.StartWithMinio(config, params, p.objStoreF)
}

//nolint:gocritic
func fPutObjectOk(ctx context.Context, bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error) {
	targetDir := fmt.Sprintf("./%s", bucketName)
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		if err := os.MkdirAll(targetDir, os.ModePerm); err != nil {
			logger.Fatalf("could not create target dir: %s, error: %s", targetDir, err.Error())
		}
	}
	fileName.Store(fmt.Sprintf("%s/%s", bucketName, "mockLog.txt"))
	f, err := os.OpenFile(fileName.Load(), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(0o777))

	if err != nil {
		logger.Panicf("could not open or create f: %s, error: %s", fileName.Load(), err.Error())
	}
	defer f.Close()

	if _, err := f.WriteString(fmt.Sprintf("%s | from '%s' to b: `%s` as obj: `%s`\n", time.Now().String(), filePath, bucketName, objectName)); err != nil {
		return 0, fmt.Errorf(err.Error())
	}

	return 1, nil
}

type putWithErr struct {
	ctx    context.Context
	cancel context.CancelFunc
}

//nolint:gocritic
func (put *putWithErr) fPutObjectErr(ctx context.Context, bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error) {
	select {
	case <-put.ctx.Done():
		targetDir := fmt.Sprintf("./%s", bucketName)
		if _, err := os.Stat(targetDir); os.IsNotExist(err) {
			if err := os.MkdirAll(targetDir, os.ModePerm); err != nil {
				logger.Fatalf("could not create target dir: %s, error: %s", targetDir, err.Error())
			}
		}

		fileName.Store(fmt.Sprintf("%s/%s", bucketName, "mockLog.txt"))
		f, err := os.OpenFile(fileName.Load(), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(0o777))
		if err != nil {
			logger.Panicf("could not open or create file: %s, error: %s", fileName, err.Error())
		}

		if _, err := f.WriteString(fmt.Sprintf("%s | from '%s' to b: `%s` as obj: `%s`\n", time.Now().String(), filePath, bucketName, objectName)); err != nil {
			return 0, fmt.Errorf(err.Error())
		}
		return 1, nil
	default:
		return 0, fmt.Errorf("fake could not sent")
	}
}

func TestStart(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long tests in short mode")
	}

	bucketName := "some"
	tests := struct {
		firstPack  []test.Msg
		secondPack []test.Msg
		thirdPack  []test.Msg
	}{
		firstPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
		secondPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
		thirdPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
	}

	// pattern for parent log file
	pattern := fmt.Sprintf("%s/*.log", dir)

	test.ClearDir(t, dir)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	s3MockClient := mock_s3.NewMockObjectStoreClient(ctl)
	s3MockClient.EXPECT().BucketExists(bucketName).Return(true, nil).AnyTimes()
	s3MockClient.EXPECT().FPutObjectWithContext(gomock.Any(), bucketName, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(fPutObjectOk).AnyTimes()

	fileConfig := file.Config{
		TargetFile:        targetFile,
		RetentionInterval: "300ms",
		Layout:            "01",
		BatchFlushTimeout: "100ms",
	}
	config := &Config{
		FileConfig:      fileConfig,
		CompressionType: "zip",
		Endpoint:        bucketName,
		AccessKey:       bucketName,
		SecretKey:       bucketName,
		DefaultBucket:   bucketName,
		Secure:          false,
	}
	test.ClearDir(t, dir)
	defer test.ClearDir(t, dir)
	test.ClearDir(t, config.DefaultBucket)
	defer test.ClearDir(t, config.DefaultBucket)
	test.NewConfig(config, map[string]int{"gomaxprocs": 1, "capacity": 64})

	p := newPipeline(t, config, func(cfg *Config) (ObjectStoreClient, map[string]ObjectStoreClient, error) {
		return s3MockClient, map[string]ObjectStoreClient{
			bucketName: s3MockClient,
		}, nil
	})
	assert.NotNil(t, p, "could not create new pipeline")
	p.Start()
	time.Sleep(300 * time.Microsecond)

	test.SendPack(t, p, tests.firstPack)
	time.Sleep(time.Second)
	size1 := test.CheckNotZero(t, fileName.Load(), "s3 data is missed after first pack")

	// check deletion upload log files
	match := test.GetMatches(t, pattern)
	assert.Equal(t, 1, len(match))
	test.CheckZero(t, match[0], "log file is not nil")

	// initial sending the second pack
	// no special situations
	test.SendPack(t, p, tests.secondPack)
	time.Sleep(time.Second)

	match = test.GetMatches(t, pattern)
	assert.Equal(t, 1, len(match))
	test.CheckZero(t, match[0], "log file is not empty")

	size2 := test.CheckNotZero(t, fileName.Load(), "s3 data missed after second pack")
	assert.True(t, size2 > size1)

	// failed during writing
	test.SendPack(t, p, tests.thirdPack)
	time.Sleep(200 * time.Millisecond)
	p.Stop()

	// check log file not empty
	match = test.GetMatches(t, pattern)
	assert.Equal(t, 1, len(match))
	test.CheckNotZero(t, match[0], "log file data missed")
}

func TestStartWithMultiBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long tests in short mode")
	}
	// will be created.
	dynamicBucket := "FAKE_BUCKET"

	buckets := []string{"main", "multi_bucket1", "multi_bucket2"}
	tests := struct {
		firstPack  []test.Msg
		secondPack []test.Msg
		thirdPack  []test.Msg
	}{
		firstPack: []test.Msg{
			// msg to main bucket.
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			// msg to first of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[1])),
			// msg to second of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[2])),
			// msg to not exist multi_bucket, will send to main bucket.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, dynamicBucket)),
			// msg to defaultBucket.
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			// msg to first of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[1])),
			// msg to second of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[2])),
		},
		secondPack: []test.Msg{
			// msg to main bucket.
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			// msg to first of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[1])),
			// msg to second of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[2])),
			// msg to not exist multi_bucket, will send to main bucket.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, dynamicBucket)),
			// msg to defaultBucket.
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			// msg to first of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[1])),
			// msg to second of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[2])),
		},
		thirdPack: []test.Msg{
			// msg to main bucket.
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			// msg to first of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[1])),
			// msg to second of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[2])),
			// msg to not exist multi_bucket, will send to main bucket.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, dynamicBucket)),
			// msg to defaultBucket.
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			// msg to first of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[1])),
			// msg to second of multi_buckets.
			test.Msg(fmt.Sprintf(`{"bucket_name": "%s", "level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`, buckets[2])),
		},
	}

	// patternMain for parent log file
	patternMain := fmt.Sprintf("%s/*.log", dir)
	patternForMultiBucket1 := fmt.Sprintf("%s%s/%s/*.log", dir, StaticBucketDir, buckets[1])
	patternForMultiBucket2 := fmt.Sprintf("%s%s/%s/*.log", dir, StaticBucketDir, buckets[2])
	patterns := []string{}
	patterns = append(append(append(patterns, patternMain), patternForMultiBucket1), patternForMultiBucket2)

	test.ClearDir(t, dir)
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	s3MockClient := mock_s3.NewMockObjectStoreClient(ctl)

	// dynamicBucket
	for _, bucketName := range buckets {
		s3MockClient.EXPECT().BucketExists(bucketName).Return(true, nil).AnyTimes()

		s3MockClient.EXPECT().FPutObjectWithContext(gomock.Any(), bucketName, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(fPutObjectOk).AnyTimes()
	}
	// at first dynamic bucket doesn't exist
	s3MockClient.EXPECT().BucketExists(dynamicBucket).Return(false, nil).Times(1)
	// successful creation on bucket
	s3MockClient.EXPECT().MakeBucket(dynamicBucket, "").Return(nil).Times(1)
	// s3 answers that bucket exists
	s3MockClient.EXPECT().BucketExists(dynamicBucket).Return(true, nil).AnyTimes()
	s3MockClient.EXPECT().FPutObjectWithContext(gomock.Any(), dynamicBucket, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(fPutObjectOk).AnyTimes()

	fileConfig := file.Config{
		TargetFile:        targetFile,
		RetentionInterval: "300ms",
		Layout:            "01",
		BatchFlushTimeout: "100ms",
	}
	config := &Config{
		FileConfig:       fileConfig,
		CompressionType:  "zip",
		Endpoint:         buckets[0],
		AccessKey:        buckets[0],
		SecretKey:        buckets[0],
		DefaultBucket:    buckets[0],
		Secure:           false,
		BucketEventField: "bucket_name",
		MultiBuckets: []singleBucketConfig{
			{
				Endpoint:       buckets[1],
				AccessKey:      buckets[1],
				SecretKey:      buckets[1],
				Bucket:         buckets[1],
				Secure:         false,
				FilePluginInfo: file.Config{},
			},
			{
				Endpoint:       buckets[2],
				AccessKey:      buckets[2],
				SecretKey:      buckets[2],
				Bucket:         buckets[2],
				Secure:         false,
				FilePluginInfo: file.Config{},
			},
		},
	}
	test.ClearDir(t, dir)
	defer test.ClearDir(t, dir)
	test.ClearDir(t, buckets[0])
	defer test.ClearDir(t, buckets[0])
	test.ClearDir(t, buckets[1])
	defer test.ClearDir(t, buckets[1])
	test.ClearDir(t, buckets[2])
	defer test.ClearDir(t, buckets[2])
	test.ClearDir(t, dynamicBucket)
	defer test.ClearDir(t, dynamicBucket)
	test.NewConfig(config, map[string]int{"gomaxprocs": 1, "capacity": 64})

	p := newPipeline(t, config, func(cfg *Config) (ObjectStoreClient, map[string]ObjectStoreClient, error) {
		return s3MockClient, map[string]ObjectStoreClient{
			buckets[0]:    s3MockClient,
			buckets[1]:    s3MockClient,
			buckets[2]:    s3MockClient,
			dynamicBucket: s3MockClient,
		}, nil
	})
	assert.NotNil(t, p, "could not create new pipeline")
	p.Start()
	time.Sleep(300 * time.Microsecond)

	test.SendPack(t, p, tests.firstPack)
	time.Sleep(time.Second)
	size1 := test.CheckNotZero(t, fileName.Load(), "s3 data is missed after first pack")

	// check deletion upload log files
	for _, pattern := range patterns {
		match := test.GetMatches(t, pattern)
		assert.Equal(t, 1, len(match), "no matches for: ", pattern)
		test.CheckZero(t, match[0], "log file is not nil")
	}

	// initial sending the second pack
	// no special situations
	test.SendPack(t, p, tests.secondPack)
	time.Sleep(time.Second)

	for _, pattern := range patterns {
		match := test.GetMatches(t, pattern)
		assert.Equal(t, 1, len(match))
		test.CheckZero(t, match[0], "log file is not empty")
	}

	size2 := test.CheckNotZero(t, fileName.Load(), "s3 data missed after second pack")
	assert.True(t, size2 > size1)

	// failed during writing
	test.SendPack(t, p, tests.thirdPack)
	time.Sleep(200 * time.Millisecond)
	p.Stop()

	// check log file not empty
	for _, pattern := range patterns {
		match := test.GetMatches(t, pattern)
		assert.Equal(t, 1, len(match))
		test.CheckNotZero(t, match[0], fmt.Sprintf("log file data missed for: %s", pattern))
	}
}

func newPipeline(t *testing.T, configOutput *Config, objStoreF objStoreFactory) *pipeline.Pipeline {
	t.Helper()
	settings := &pipeline.Settings{
		Capacity:            4096,
		MaintenanceInterval: time.Second * 10,
		// MaintenanceInterval: time.Second * 100000,
		AntispamThreshold:  0,
		AvgEventSize:       2048,
		StreamField:        "stream",
		Decoder:            "json",
		MetricHoldDuration: pipeline.DefaultMetricHoldDuration,
	}

	p := pipeline.New("test_pipeline", settings, prometheus.NewRegistry(), zap.NewNop())
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
	anyPlugin, _ = testFactory(objStoreF)
	outputPlugin := anyPlugin.(*testS3Plugin)
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
		DefaultBucket:   "some",
		Secure:          false,
	}
	test.ClearDir(t, dir)
	defer test.ClearDir(t, dir)
	test.ClearDir(t, config.DefaultBucket)
	defer test.ClearDir(t, config.DefaultBucket)
	test.NewConfig(config, map[string]int{"gomaxprocs": 1, "capacity": 64})

	p := newPipeline(t, config, func(cfg *Config) (ObjectStoreClient, map[string]ObjectStoreClient, error) {
		return nil, nil, nil
	})

	assert.NotNil(t, p, "could not create new pipeline")

	assert.Panics(t, p.Start)
}

func TestStartWithSendProblems(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long tests in short mode")
	}

	bucketName := "some"
	tests := struct {
		firstPack  []test.Msg
		secondPack []test.Msg
		thirdPack  []test.Msg
	}{
		firstPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
		secondPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
		thirdPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
	}

	// pattern for parent log file
	pattern := fmt.Sprintf("%s/*.log", dir)
	zipPattern := fmt.Sprintf("%s/*.zip", dir)

	writeFileSleep := 100*time.Millisecond + 100*time.Millisecond
	sealUpFileSleep := 2*200*time.Millisecond + 500*time.Millisecond

	test.ClearDir(t, dir)
	assert.Equal(t, 0, len(test.GetMatches(t, zipPattern)))

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	s3MockClient := mock_s3.NewMockObjectStoreClient(ctl)
	s3MockClient.EXPECT().BucketExists(bucketName).Return(true, nil).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	withErr := putWithErr{ctx: ctx, cancel: cancel}
	s3MockClient.EXPECT().FPutObjectWithContext(gomock.Any(), bucketName, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(withErr.fPutObjectErr).AnyTimes()

	fileConfig := file.Config{
		TargetFile:        targetFile,
		RetentionInterval: "300ms",
		Layout:            "01",
		BatchFlushTimeout: "100ms",
	}
	config := &Config{
		FileConfig:      fileConfig,
		CompressionType: "zip",
		Endpoint:        bucketName,
		AccessKey:       bucketName,
		SecretKey:       bucketName,
		DefaultBucket:   bucketName,
		Secure:          false,
	}
	test.ClearDir(t, dir)
	defer test.ClearDir(t, dir)
	test.ClearDir(t, fmt.Sprintf("%s/", config.DefaultBucket))
	defer test.ClearDir(t, config.DefaultBucket)
	test.NewConfig(config, map[string]int{"gomaxprocs": 1, "capacity": 64})
	p := newPipeline(t, config, func(cfg *Config) (ObjectStoreClient, map[string]ObjectStoreClient, error) {
		return s3MockClient, map[string]ObjectStoreClient{
			bucketName: s3MockClient,
		}, nil
	})

	assert.NotNil(t, p, "could not create new pipeline")

	p.Start()
	time.Sleep(300 * time.Microsecond)
	test.SendPack(t, p, tests.firstPack)
	time.Sleep(writeFileSleep)
	time.Sleep(sealUpFileSleep)

	assert.True(t, isNoSentToS3())

	matches := test.GetMatches(t, zipPattern)

	assert.Equal(t, 1, len(matches), strings.Join(matches, ", "))
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

	assert.True(t, isNoSentToS3())

	cancel()

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
	test.CheckNotZero(t, fileName.Load(), "s3 file is empty")
	f, err := os.Open(fileName.Load())

	assert.NoError(t, err)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineCounter := 0
	for scanner.Scan() {
		lineCounter++
	}
	assert.GreaterOrEqual(t, lineCounter, 3)
}

func isNoSentToS3() bool {
	_, err := os.Stat(fileName.Load())
	return err != nil && os.IsNotExist(err)
}
