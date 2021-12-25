package usecase

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/minio/minio-go"

	"github.com/ozonru/file.d/plugin/output/s3"

	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/plugin/output/file"
)

var (
	ErrCreateOutputPluginCantCheckBucket = errors.New("could not check bucket")
	ErrCreateOutputPluginNoSuchBucket    = errors.New("bucket doesn't exist")
)

type objStoreFactory func(cfg *Config) (s3.ObjectStoreClient, map[string]s3.ObjectStoreClient, error)

func (p *Plugin) minioClientsFactory(cfg *Config) (s3.ObjectStoreClient, map[string]s3.ObjectStoreClient, error) {
	minioClients := make(map[string]s3.ObjectStoreClient)
	// initialize minio clients object for main bucket.
	defaultClient, err := minio.New(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Secure)
	if err != nil {
		return nil, nil, err
	}

	for _, singleBucket := range cfg.MultiBuckets {
		client, err := minio.New(singleBucket.Endpoint, singleBucket.AccessKey, singleBucket.SecretKey, singleBucket.Secure)
		if err != nil {
			return nil, nil, err
		}
		minioClients[singleBucket.Bucket] = client
	}

	minioClients[cfg.DefaultBucket] = defaultClient
	return defaultClient, minioClients, nil
}

func (p *Plugin) getDirs(outPlugCount int) map[string]string {
	// dir for all bucket files.
	dir, _ := filepath.Split(p.config.FileConfig.TargetFile)

	targetDirs := make(map[string]string, outPlugCount)
	targetDirs[p.config.DefaultBucket] = dir
	// multi_buckets from config are sub dirs on in Config.FileConfig.TargetFile dir.
	for _, singleBucket := range p.config.MultiBuckets {
		targetDirs[singleBucket.Bucket] = filepath.Join(dir, StaticBucketDir, singleBucket.Bucket) + dirSep
	}
	return targetDirs
}

func (p *Plugin) getFileNames(outPlugCount int) map[string]string {
	fileNames := make(map[string]string, outPlugCount)

	_, f := filepath.Split(p.config.FileConfig.TargetFile)
	p.fileExtension = filepath.Ext(f)

	mainFileName := f[0 : len(f)-len(p.fileExtension)]
	fileNames[p.config.DefaultBucket] = mainFileName
	for _, singleB := range p.config.MultiBuckets {
		fileNames[singleB.Bucket] = singleB.Bucket
	}
	return fileNames
}

// Try to create buckets from dirs lying in dynamic_dirs route
func (p *Plugin) createPlugsFromDynamicBucketArtifacts(targetDirs map[string]string) {
	dynamicDirsPath := filepath.Join(targetDirs[p.config.DefaultBucket], DynamicBucketDir)
	dynamicDir, err := ioutil.ReadDir(dynamicDirsPath)
	if err != nil {
		p.logger.Infof("%s doesn't exist, willn't restore dynamic s3 buckets", err.Error())
		return
	}
	for _, f := range dynamicDir {
		if !f.IsDir() {
			continue
		}
		created := p.tryRunNewPlugin(f.Name())
		if created {
			p.logger.Infof("dynamic bucket '%s' retrieved from artifacts", f.Name())
		} else {
			p.logger.Infof("can't retrieve dynamic bucket from artifacts: %s", err.Error())
		}
	}
}

func (p *Plugin) createOutPlugin(bucketName string) (*file.Plugin, error) {
	exists, err := p.clients[bucketName].BucketExists(bucketName)
	if err != nil {
		return nil, fmt.Errorf("%w %s with error: %s", ErrCreateOutputPluginCantCheckBucket, bucketName, err.Error())
	}
	if !exists {
		return nil, fmt.Errorf("%w %s ", ErrCreateOutputPluginNoSuchBucket, bucketName)
	}

	anyPlugin, _ := file.Factory()
	outPlugin := anyPlugin.(*file.Plugin)
	outPlugin.SealUpCallback = p.addFileJobWithBucket(bucketName)

	return outPlugin, nil
}

func (p *Plugin) startPlugins(Params *pipeline.OutputPluginParams, outPlugCount int, targetDirs, fileNames map[string]string) error {
	outPlugins := make(map[string]file.PluginInterface, outPlugCount)
	outPlugin, err := p.createOutPlugin(p.config.DefaultBucket)
	if err != nil {
		return err
	}
	outPlugins[p.config.DefaultBucket] = outPlugin
	p.logger.Infof("bucket %s exists", p.config.DefaultBucket)

	p.createPlugsFromDynamicBucketArtifacts(targetDirs)
	// If multi_buckets described on file.d config, check each of them as well.
	for _, singleBucket := range p.config.MultiBuckets {
		outPlugin, err := p.createOutPlugin(singleBucket.Bucket)
		if err != nil {
			return err
		}
		outPlugins[singleBucket.Bucket] = outPlugin
		p.logger.Infof("bucket %s exists", singleBucket.Bucket)
	}

	p.logger.Info("outPlugins are ready")
	p.outPlugins = file.NewFilePlugins(outPlugins)

	starterMap := make(pipeline.PluginsStarterMap, outPlugCount)
	for bucketName := range outPlugins {
		var starterData pipeline.PluginsStarterData
		// for defaultBucket set it's config.
		if bucketName == p.config.DefaultBucket {
			starterData = pipeline.PluginsStarterData{
				Config: &p.config.FileConfig,
				Params: Params,
			}
		} else {
			// For multi_buckets copy main config and replace file path with bucket sub dir path.
			// Example /var/log/file.d.log => /var/log/static_bucket/bucketName/bucketName.log
			localBucketConfig := p.config.FileConfig
			localBucketConfig.TargetFile = fmt.Sprintf("%s%s%s", targetDirs[bucketName], fileNames[bucketName], p.fileExtension)
			starterData = pipeline.PluginsStarterData{
				Config: &localBucketConfig,
				Params: Params,
			}
		}

		starterMap[bucketName] = starterData
	}
	p.outPlugins.Start(starterMap)

	return nil
}
