package s3

import (
	"sync"

	"github.com/minio/minio-go"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/file"
	"github.com/ozontech/file.d/plugin/output/kafka"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type ObjectStoreClient interface {
	MakeBucket(bucketName string, location string) (err error)
	BucketExists(bucketName string) (bool, error)
	FPutObject(bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error)
}

type compressor interface {
	compress(archiveName, fileName string)
	getObjectOptions() minio.PutObjectOptions
	getExtension() string
	getName(fileName string) string
}

type fileDTO struct {
	fileName   string
	bucketName string
}

type singleBucketConfig struct {
	// s3 section
	Endpoint       string      `json:"endpoint" required:"true"`
	AccessKey      string      `json:"access_key" required:"true"`
	SecretKey      string      `json:"secret_key" required:"true"`
	Bucket         string      `json:"bucket" required:"true"`
	Secure         bool        `json:"secure" default:"false"`
	FilePluginInfo file.Config `json:"file_plugin" required:"true"`
}

type MultiBuckets []singleBucketConfig

type commitConfig struct {
	CommitterType string        `json:"type" default:"unknown"`
	KafkaCfg      *kafka.Config `json:"kafka_config"`
}

type Plugin struct {
	controller    pipeline.OutputPluginController
	logger        *zap.SugaredLogger
	config        *Config
	params        *pipeline.OutputPluginParams
	fileExtension string

	// defaultClient separated from others to avoid locks with dynamic buckets.
	defaultClient ObjectStoreClient
	clients       map[string]ObjectStoreClient
	limiter       *ObjectStoreClientLimiter

	outPlugins            *file.Plugins
	dynamicPlugCreationMu sync.Mutex

	compressCh chan fileDTO
	uploadCh   chan fileDTO
	compressor compressor

	commitMode      bool
	commiterPlugin  pipeline.OutputPlugin
	inputController pipeline.InputPluginController
	isNew           atomic.Bool
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//> Under the hood this plugin uses /plugin/output/file/ to collect logs
	FileConfig file.Config `json:"file_config" child:"true"` //*

	//> @3@4@5@6
	//> Compression type
	CompressionType string `json:"compression_type" default:"zip" options:"zip"` //*

	// s3 section

	//> @3@4@5@6
	//> Endpoint address of default bucket.
	Endpoint string `json:"endpoint" required:"true"` //*
	//> @3@4@5@6
	//> s3 access key.
	AccessKey string `json:"access_key" required:"true"` //*
	//> @3@4@5@6
	//> s3 secret key.
	SecretKey string `json:"secret_key" required:"true"` //*
	//> @3@4@5@6
	//>  s3 default bucket.
	DefaultBucket string `json:"bucket" required:"true"` //*
	//> @3@4@5@6
	//> MultiBuckets is additional buckets, which can also receive event.
	//> Event must contain `bucket_name` field which value is s3 bucket name.
	//> Events without `bucket_name` sends to DefaultBucket.
	MultiBuckets `json:"multi_buckets" required:"false"` //*
	//> @3@4@5@6
	//> s3 connection secure option.
	Secure bool `json:"secure" default:"false"` //*
	//> @3@4@5@6
	//> BucketEventField field change destination bucket of event to fields value.
	//> Fallback to DefaultBucket if BucketEventField bucket doesn't exist.
	BucketEventField string `json:"bucket_field_event" default:""` //*
	//> @3@4@5@6
	//> DynamicBucketsLimit regulates how many buckets can be created dynamically.
	//> Prevents problems when some random strings in BucketEventField where
	DynamicBucketsLimit int `json:"dynamic_buckets_limit" default:"32"` //*

	// 2 phase commit section

	//> @3@4@5@6
	//> Describes config of commit.
	CommitCfg *commitConfig `json:"commit_config"` //*
}
