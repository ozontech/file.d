package s3

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/minio/minio-go"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/file"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
Sends events to s3 output of one or multiple buckets.
`bucket` is default bucket for events. Addition buckets can be described in `multi_buckets` section, example down here.
Field "bucket_field_event" is filed name, that will be searched in event.
If appears we try to send event to this bucket instead of described here.

> ⚠ Currently bucket names for bucket and multi_buckets can't intersect.

> ⚠ If dynamic bucket moved to config it can leave some not send data behind.
> To send this data to s3 move bucket dir from /var/log/dynamic_buckets/bucketName to /var/log/static_buckets/bucketName (/var/log is default path)
> and restart file.d

**Example**
Standard example:
```yaml
pipelines:
  mkk:
    settings:
      capacity: 128
    # input plugin is not important in this case, let's emulate http input.
    input:
      type: http
      emulate_mode: "no"
      address: ":9200"
	actions:
	- type: json_decode
		field: message
    output:
      type: s3
      file_config:
        retention_interval: 10s
      # endpoint, access_key, secret_key, bucket are required.
      endpoint: "s3.fake_host.org:80"
      access_key: "access_key1"
      secret_key: "secret_key2"
      bucket: "bucket-logs"
      bucket_field_event: "bucket_name"
```

Example with fan-out buckets:
```yaml
pipelines:
  mkk:
    settings:
      capacity: 128
    # input plugin is not important in this case, let's emulate http input.
    input:
      type: http
      emulate_mode: "no"
      address: ":9200"
	actions:
	- type: json_decode
		field: message
    output:
      type: s3
      file_config:
        retention_interval: 10s
      # endpoint, access_key, secret_key, bucket are required.
      endpoint: "s3.fake_host.org:80"
      access_key: "access_key1"
      secret_key: "secret_key2"
      bucket: "bucket-logs"
      # bucket_field_event - event with such field will be sent to bucket with its value
      # if such exists: {"bucket_name": "secret", "message": 123} to bucket "secret".
      bucket_field_event: "bucket_name"
      # multi_buckets is optional, contains array of buckets.
      multi_buckets:
        - endpoint: "otherS3.fake_host.org:80"
          access_key: "access_key2"
          secret_key: "secret_key2"
          bucket: "bucket-logs-2"
        - endpoint: "yet_anotherS3.fake_host.ru:80"
          access_key: "access_key3"
          secret_key: "secret_key3"
          bucket: "bucket-logs-3"
```
}*/

const (
	fileNameSeparator = "_"
	dirSep            = "/"
	StaticBucketDir   = "static_buckets"
	DynamicBucketDir  = "dynamic_buckets"
)

var (
	compressors = map[string]func(*zap.SugaredLogger) compressor{
		zipName: newZipCompressor,
	}
)

type ObjectStoreClient interface {
	MakeBucket(bucketName string, location string) (err error)
	BucketExists(bucketName string) (bool, error)
	FPutObjectWithContext(ctx context.Context, bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error)
}

type compressor interface {
	compress(archiveName, fileName string)
	getObjectOptions() minio.PutObjectOptions
	getExtension() string
	getName(fileName string) string
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

	// plugin metrics
	sendErrorMetric  prometheus.Counter
	uploadFileMetric *prometheus.CounterVec

	rnd   rand.Rand
	rndMx sync.Mutex
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

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Under the hood this plugin uses /plugin/output/file/ to collect logs.
	FileConfig file.Config `json:"file_config"` // *

	// > @3@4@5@6
	// >
	// > Compressed files format.
	CompressionType string `json:"compression_type" default:"zip" options:"zip"` // *

	// s3 section
	// > @3@4@5@6
	// >
	// > Address of default bucket.
	Endpoint string `json:"endpoint" required:"true"` // *

	// > @3@4@5@6
	// >
	// > S3 access key.
	AccessKey string `json:"access_key" required:"true"` // *

	// > @3@4@5@6
	// >
	// > S3 secret key.
	SecretKey string `json:"secret_key" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Main S3 bucket.
	DefaultBucket string `json:"bucket" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Additional buckets, which can also receive event.
	// > Event with bucket_name field sends to such s3 bucket.
	MultiBuckets `json:"multi_buckets" required:"false"` // *

	// > @3@4@5@6
	// >
	// > S3 connection secure option.
	Secure bool `json:"secure" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Change destination bucket of event.
	// > Fallback to DefaultBucket if BucketEventField bucket doesn't exist.
	BucketEventField string `json:"bucket_field_event" default:""` // *

	// > @3@4@5@6
	// >
	// > Regulates number of buckets that can be created dynamically.
	DynamicBucketsLimit int `json:"dynamic_buckets_limit" default:"32"` // *

	// > @3@4@5@6
	// >
	// > Sets upload timeout.
	UploadTimeout  cfg.Duration `json:"upload_timeout" default:"1m" parse:"duration"` // *
	UploadTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Retries of upload. If File.d cannot upload for this number of attempts,
	// > File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).
	Retry int `json:"retry" default:"10"` // *

	// > @3@4@5@6
	// >
	// > After an insert error, fall with a non-zero exit code or not
	// > **Experimental feature**
	FatalOnFailedInsert bool `json:"fatal_on_failed_insert" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Retention milliseconds for retry to upload.
	Retention  cfg.Duration `json:"retention" default:"1s" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Multiplier for exponential increase of retention between retries
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *
}

func (c *Config) IsMultiBucketExists(bucketName string) bool {
	if c.MultiBuckets == nil {
		return false
	}

	for i := range c.MultiBuckets {
		bucket := &c.MultiBuckets[i]
		if bucketName == bucket.Bucket {
			return true
		}
	}

	return false
}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    "s3",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.rnd = *rand.New(rand.NewSource(time.Now().UnixNano()))
	p.registerMetrics(params.MetricCtl)
	p.StartWithMinio(config, params, p.minioClientsFactory)
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounter("output_s3_send_error", "Total s3 send errors")
	p.uploadFileMetric = ctl.RegisterCounterVec("output_s3_upload_file", "Total files upload", "bucket_name")
}

func (p *Plugin) StartWithMinio(config pipeline.AnyConfig, params *pipeline.OutputPluginParams, factory objStoreFactory) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.config = config.(*Config)
	p.params = params

	// outPlugCount is defaultBucket + multi_buckets count, use to set maps size.
	outPlugCount := len(p.config.MultiBuckets) + 1
	p.limiter = NewObjectStoreClientLimiter(p.config.DynamicBucketsLimit + outPlugCount)

	// set up compression.
	newCompressor, ok := compressors[p.config.CompressionType]
	if !ok {
		p.logger.Fatalf("compression type: %s is not supported", p.config.CompressionType)
	}
	p.compressor = newCompressor(p.logger)

	// dir for all bucket files.
	targetDirs, err := p.getStaticDirs(outPlugCount)
	if err != nil {
		p.logger.Fatal(err)
	}

	// initialize minio clients.
	defaultClient, clients, err := factory(p.config)
	if err != nil {
		p.logger.Panicf("could not create minio client, error: %s", err.Error())
	}
	p.defaultClient = defaultClient
	p.clients = clients

	// dynamicDirs needs defaultClient set.
	dynamicDirs := p.getDynamicDirsArtifacts(targetDirs)
	// file for each bucket.
	fileNames := p.getFileNames(outPlugCount)

	p.uploadCh = make(chan fileDTO, p.config.FileConfig.WorkersCount_*4)
	p.compressCh = make(chan fileDTO, p.config.FileConfig.WorkersCount_)

	for i := 0; i < p.config.FileConfig.WorkersCount_; i++ {
		go p.uploadWork(pipeline.GetBackoff(
			p.config.Retention_,
			float64(p.config.RetentionExponentMultiplier),
			uint64(p.config.Retry),
		))
		go p.compressWork()
	}
	err = p.startPlugins(params, outPlugCount, targetDirs, fileNames)
	if err != nil {
		p.logger.Fatal("can't start plugin", zap.Error(err))
	}

	p.uploadExistingFiles(targetDirs, dynamicDirs, fileNames)
	p.logger.Info("old files uploaded")
}

func (p *Plugin) Stop() {
	p.outPlugins.Stop()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.outPlugins.Out(event, pipeline.PluginSelector{
		CondType:  pipeline.ByNameSelector,
		CondValue: p.getBucketName(event),
	})
}

// getBucketName decides which s3 bucket shall receive event.
func (p *Plugin) getBucketName(event *pipeline.Event) string {
	bucketName := pipeline.CloneString(event.Root.Dig(p.config.BucketEventField).AsString())

	// no BucketEventField in message, it's DefaultBucket, showtime
	if bucketName == "" {
		return p.config.DefaultBucket
	}

	if p.outPlugins.Exists(bucketName) {
		return bucketName
	}

	// try to create dynamic bucketName
	if created := p.tryRunNewPlugin(bucketName); created {
		// succeed, return new bucketName
		return bucketName
	}

	// failed to create, fallback on DefaultBucket
	return p.config.DefaultBucket
}

func (p *Plugin) getDynamicDirsArtifacts(targetDirs map[string]string) map[string]string {
	dynamicDirs := make(map[string]string)

	dynamicDirsPath := filepath.Join(targetDirs[p.config.DefaultBucket], DynamicBucketDir)
	dynamicDir, err := os.ReadDir(dynamicDirsPath)
	// If no such dir, no dynamic dirs existed.
	if err != nil {
		return nil
	}

	for _, dynDir := range dynamicDir {
		// our target only dirs.
		if !dynDir.IsDir() {
			continue
		}
		_, ok := targetDirs[dynDir.Name()]
		// ignore if we have such static dir.
		if ok {
			continue
		}
		_, err := p.defaultClient.BucketExists(dynDir.Name())
		if err != nil {
			continue
		}

		dynamicDirs[dynDir.Name()] = path.Join(dynamicDirsPath, dynDir.Name()) + dirSep
	}

	return dynamicDirs
}

// creates new dynamic plugin if such s3 bucket exists.
func (p *Plugin) tryRunNewPlugin(bucketName string) (isCreated bool) {
	// To avoid concurrent creation of bucketName plugin.
	p.dynamicPlugCreationMu.Lock()
	defer p.dynamicPlugCreationMu.Unlock()
	// Probably other worker created plugin concurrently, need check dynamic bucket one more time.
	if p.outPlugins.IsDynamic(bucketName) {
		return true
	}
	// If limit of dynamic buckets reached fallback to DefaultBucket.
	if !p.limiter.CanCreate() {
		p.logger.Warn(
			"Limit of %d dynamic buckets reached, can't create new. Fallback to %s.",
			p.config.DynamicBucketsLimit,
			p.config.DefaultBucket,
		)
		return false
	}

	defaultBucketClient := p.defaultClient
	exists, err := defaultBucketClient.BucketExists(bucketName)
	// Fallback to DefaultBucket if we failed check bucket existence.
	if err != nil {
		p.logger.Errorf("couldn't check bucket from message existence: %s", err.Error())
		return false
	}
	if !exists {
		err := defaultBucketClient.MakeBucket(bucketName, "")
		if err != nil {
			p.logger.Errorf("can't create s3 bucket %s, error: %v, fallback to %s", bucketName, err, p.config.DefaultBucket)
			return false
		}
	}

	dir, _ := filepath.Split(p.config.FileConfig.TargetFile)
	bucketDir := filepath.Join(dir, DynamicBucketDir, bucketName) + dirSep
	// dynamic bucket share s3 credentials with DefaultBucket.
	anyPlugin, _ := file.Factory()
	outPlugin := anyPlugin.(*file.Plugin)
	outPlugin.SealUpCallback = p.addFileJobWithBucket(bucketName)

	localBucketConfig := p.config.FileConfig
	localBucketConfig.TargetFile = fmt.Sprintf("%s%s%s", bucketDir, bucketName, p.fileExtension)
	outPlugin.Start(&localBucketConfig, p.params)

	p.outPlugins.Add(bucketName, outPlugin)
	p.limiter.Increment()

	return true
}

// uploadExistingFiles gets files from dirs, sorts it, compresses it if it's need, and then upload to s3.
func (p *Plugin) uploadExistingFiles(targetDirs, dynamicDirs, fileNames map[string]string) {
	allDirs := make(map[string]string, len(dynamicDirs)+len(targetDirs))
	for k, v := range dynamicDirs {
		allDirs[k] = v
	}
	for k, v := range targetDirs {
		allDirs[k] = v
	}
	for bucketName, dir := range allDirs {
		// get all compressed files.
		pattern := fmt.Sprintf("%s*%s", dir, p.compressor.getExtension())
		compressedFiles, err := filepath.Glob(pattern)
		if err != nil {
			p.logger.Panicf("could not read dir: %s", dir)
		}
		// sort compressed files by creation time.
		sort.Slice(compressedFiles, p.getSortFunc(compressedFiles))
		// upload archive.
		for _, z := range compressedFiles {
			p.logger.Infof("uploaded file: %s, bucket: %s", z, bucketName)
			p.uploadCh <- fileDTO{fileName: z, bucketName: bucketName}
		}
		// compress all files that we have in the dir
		p.compressFilesInDir(bucketName, targetDirs, fileNames)
	}
	p.logger.Info("exited files uploaded")
}

// compressFilesInDir compresses all files in dir
func (p *Plugin) compressFilesInDir(bucketName string, targetDirs, fileNames map[string]string) {
	pattern := fmt.Sprintf("%s/%s%s*%s*%s", targetDirs[bucketName], fileNames[bucketName], fileNameSeparator, fileNameSeparator, p.fileExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		p.logger.Panicf("could not read dir: %s", targetDirs[bucketName])
	}
	// sort files by creation time.
	sort.Slice(files, p.getSortFunc(files))
	for _, f := range files {
		p.compressCh <- fileDTO{fileName: f, bucketName: bucketName}
	}
}

// getSortFunc return func that sorts files by mod time
func (p *Plugin) getSortFunc(files []string) func(i, j int) bool {
	return func(i, j int) bool {
		InfoI, err := os.Stat(files[i])
		if err != nil {
			p.logger.Panicf("could not get info about file: %s, error: %s", files[i], err.Error())
		}
		InfoJ, err := os.Stat(files[j])
		if err != nil {
			p.logger.Panicf("could not get info about file: %s, error: %s", files[j], err.Error())
		}
		return InfoI.ModTime().Before(InfoJ.ModTime())
	}
}

func (p *Plugin) addFileJobWithBucket(bucketName string) func(filename string) {
	return func(filename string) {
		p.compressCh <- fileDTO{fileName: filename, bucketName: bucketName}
	}
}

func (p *Plugin) uploadWork(workerBackoff backoff.BackOff) {
	for compressed := range p.uploadCh {
		workerBackoff.Reset()
		err := backoff.Retry(func() error {
			p.logger.Infof("starting upload s3 object. fileName=%s, bucketName=%s", compressed.fileName, compressed.bucketName)
			err := p.uploadToS3(compressed)
			if err == nil {
				p.uploadFileMetric.WithLabelValues(compressed.bucketName).Inc()
				p.logger.Infof("successfully uploaded object=%s", compressed.fileName)
				// delete archive after uploading
				err = os.Remove(compressed.fileName)
				if err != nil && !os.IsNotExist(err) {
					p.logger.Panicf("could not delete file: %s, err: %s", compressed, err.Error())
				}
				return nil
			}
			p.logger.Errorf("could not upload object: %s, error: %s", compressed, err.Error())
			return err
		}, workerBackoff)

		if err != nil {
			var level zapcore.Level
			if p.config.FatalOnFailedInsert {
				level = zapcore.FatalLevel
			} else {
				level = zapcore.ErrorLevel
			}

			p.logger.Desugar().Log(level, "could not upload s3 object", zap.Error(err),
				zap.Int("retries", p.config.Retry),
			)
		}
	}
}

// compressWork compress file from channel and then delete source file
func (p *Plugin) compressWork() {
	for dto := range p.compressCh {
		p.logger.Infof("compress fileName=%s, bucketName=%s", dto.fileName, dto.bucketName)

		compressedName := p.compressor.getName(dto.fileName)
		p.compressor.compress(compressedName, dto.fileName)
		// delete old file
		if err := os.Remove(dto.fileName); err != nil && !os.IsNotExist(err) {
			p.logger.Panicf("could not delete file: %s, error: %s", dto, err.Error())
		}
		dto.fileName = compressedName
		p.uploadCh <- fileDTO{fileName: dto.fileName, bucketName: dto.bucketName}
	}
}

func (p *Plugin) uploadToS3(compressedDTO fileDTO) error {
	var cl ObjectStoreClient
	if ok := p.outPlugins.IsStatic(compressedDTO.bucketName); ok {
		cl = p.clients[compressedDTO.bucketName]
	} else {
		cl = p.defaultClient
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.config.UploadTimeout_)
	defer cancel()

	_, err := cl.FPutObjectWithContext(
		ctx,
		compressedDTO.bucketName, p.generateObjectName(compressedDTO.fileName),
		compressedDTO.fileName,
		p.compressor.getObjectOptions(),
	)

	if err != nil {
		p.sendErrorMetric.Inc()
		return fmt.Errorf("could not upload file: %s into bucket: %s, error: %s", compressedDTO.fileName, compressedDTO.bucketName, err.Error())
	}
	return nil
}

// generateObjectName generates object name by compressed file name
func (p *Plugin) generateObjectName(name string) string {
	n := strconv.FormatInt(p.nextRandomValue(), 16)
	n = n[len(n)-8:]
	objectName := path.Base(name)
	objectName = objectName[0 : len(objectName)-len(p.compressor.getExtension())]
	return fmt.Sprintf("%s.%s%s", objectName, n, p.compressor.getExtension())
}

func (p *Plugin) nextRandomValue() int64 {
	p.rndMx.Lock()
	defer p.rndMx.Unlock()
	return p.rnd.Int63n(math.MaxInt64)
}
