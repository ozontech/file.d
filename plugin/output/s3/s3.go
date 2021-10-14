package s3

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/minio/minio-go"
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/longpanic"
	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/plugin/output/file"
	"go.uber.org/zap"
)

const (
	fileNameSeparator  = "_"
	attemptIntervalMin = 1 * time.Second
)

var (
	attemptInterval = attemptIntervalMin
	compressors     = map[string]func(*zap.SugaredLogger) compressor{
		zipName: newZipCompressor,
	}

	r = rand.New(rand.NewSource(time.Now().UnixNano()))
)

type objectStoreClient interface {
	BucketExists(bucketName string) (bool, error)
	FPutObject(bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error)
}

type compressor interface {
	compress(archiveName, fileName string)
	getObjectOptions() minio.PutObjectOptions
	getExtension() string
	getName(fileName string) string
}

type Plugin struct {
	controller pipeline.OutputPluginController
	logger     *zap.SugaredLogger
	config     *Config
	client     objectStoreClient
	outPlugin  *file.Plugin

	targetDir     string
	fileExtension string
	fileName      string

	compressCh chan string
	uploadCh   chan string

	compressor compressor
}

type Config struct {
	// Under the hood this plugin uses /plugin/output/file/ to collect logs
	FileConfig file.Config `json:"file_config" child:"true"`

	// Compression type
	CompressionType string `json:"compression_type" default:"zip" options:"zip"`

	// s3 section
	Endpoint  string `json:"endpoint" required:"true"`
	AccessKey string `json:"access_key" required:"true"`
	SecretKey string `json:"secret_key" required:"true"`
	Bucket    string `json:"bucket" required:"true"`
	Secure    bool   `json:"secure" default:"false"`

	// for mock client injection
	client *objectStoreClient
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
	p.controller = params.Controller
	p.logger = params.Logger
	p.config = config.(*Config)

	// set up compression
	newCompressor, ok := compressors[p.config.CompressionType]
	if !ok {
		p.logger.Fatalf("compression type: %s is not supported", p.config.CompressionType)
	}
	p.compressor = newCompressor(p.logger)

	dir, f := filepath.Split(p.config.FileConfig.TargetFile)

	p.targetDir = dir
	p.fileExtension = filepath.Ext(f)
	p.fileName = f[0 : len(f)-len(p.fileExtension)]

	p.uploadCh = make(chan string, p.config.FileConfig.WorkersCount_*4)
	p.compressCh = make(chan string, p.config.FileConfig.WorkersCount_)

	for i := 0; i < p.config.FileConfig.WorkersCount_; i++ {
		longpanic.Go(p.uploadWork)
		longpanic.Go(p.compressWork)
	}

	// initialize minio client object.
	minioClient, err := minio.New(p.config.Endpoint, p.config.AccessKey, p.config.SecretKey, p.config.Secure)
	if err != nil || minioClient == nil {
		p.logger.Panicf("could not create minio client, error: %s", err.Error())
	}
	p.client = minioClient

	if p.config.client != nil {
		p.logger.Info("set mock client")
		p.client = *p.config.client
	}

	exist, err := p.client.BucketExists(p.config.Bucket)
	if err != nil {
		p.logger.Panicf("could not check bucket: %s, error: %s", p.config.Bucket, err.Error())
	}
	if !exist {
		p.logger.Fatalf("bucket: %s, does not exist", p.config.Bucket)
	}
	p.logger.Info("client is ready")
	p.logger.Infof("bucket: %s exists", p.config.Bucket)

	anyPlugin, _ := file.Factory()
	p.outPlugin = anyPlugin.(*file.Plugin)

	p.outPlugin.SealUpCallback = p.addFileJob

	p.outPlugin.Start(&p.config.FileConfig, params)
	p.uploadExistingFiles()
}

func (p *Plugin) Stop() {
	p.outPlugin.Stop()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.outPlugin.Out(event)
}

// uploadExistingFiles gets files from dirs, sorts it, compresses it if it's need, and then upload to s3
func (p *Plugin) uploadExistingFiles() {
	// get all compressed files
	pattern := fmt.Sprintf("%s*%s", p.targetDir, p.compressor.getExtension())
	compressedFiles, err := filepath.Glob(pattern)
	if err != nil {
		p.logger.Panicf("could not read dir: %s", p.targetDir)
	}
	// sort compressed files by creation time
	sort.Slice(compressedFiles, p.getSortFunc(compressedFiles))
	// upload archive
	for _, z := range compressedFiles {
		p.uploadCh <- z
	}

	// compress all files that we have in the dir
	p.compressFilesInDir()
}

// compressFilesInDir compresses all files in dir
func (p *Plugin) compressFilesInDir() {
	pattern := fmt.Sprintf("%s/%s%s*%s*%s", p.targetDir, p.fileName, fileNameSeparator, fileNameSeparator, p.fileExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		p.logger.Panicf("could not read dir: %s", p.targetDir)
	}
	// sort files by creation time
	sort.Slice(files, p.getSortFunc(files))
	for _, f := range files {
		p.compressCh <- f
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

func (p *Plugin) addFileJob(fileName string) {
	p.compressCh <- fileName
}

// uploadWork uploads compressed files from channel to s3 and then delete compressed file
// in case error worker will attempt sending with an exponential time interval
func (p *Plugin) uploadWork() {
	for compressed := range p.uploadCh {
		sleepTime := attemptInterval
		for {
			err := p.uploadToS3(compressed)
			if err == nil {
				p.logger.Infof("successfully uploaded object: %s", compressed)
				// delete archive after uploading
				err = os.Remove(compressed)
				if err != nil {
					p.logger.Panicf("could not delete file: %s, err: %s", compressed, err.Error())
				}
				break
			}
			sleepTime += sleepTime
			p.logger.Errorf("could not upload object: %s, next attempt in %s, error: %s", compressed, sleepTime.String(), err.Error())
			time.Sleep(sleepTime)
		}
	}
}

// compressWork compress file from channel and then delete source file
func (p *Plugin) compressWork() {
	for f := range p.compressCh {
		compressedName := p.compressor.getName(f)
		p.compressor.compress(compressedName, f)
		// delete old file
		if err := os.Remove(f); err != nil {
			p.logger.Panicf("could not delete file: %s, error: %s", f, err.Error())
		}
		p.uploadCh <- compressedName
	}
}

// uploadToS3 uploads compressed file to s3
func (p *Plugin) uploadToS3(name string) error {
	_, err := p.client.FPutObject(p.config.Bucket, p.generateObjectName(name), name, p.compressor.getObjectOptions())
	if err != nil {
		return fmt.Errorf("could not upload file: %s into bucket: %s, error: %s", name, p.config.Bucket, err.Error())
	}
	return nil
}

// generateObjectName generates object name by compressed file name
func (p *Plugin) generateObjectName(name string) string {
	n := strconv.FormatInt(r.Int63n(math.MaxInt64), 16)
	n = n[len(n)-8:]
	objectName := path.Base(name)
	objectName = objectName[0 : len(objectName)-len(p.compressor.getExtension())]
	return fmt.Sprintf("%s.%s%s", objectName, n, p.compressor.getExtension())
}
