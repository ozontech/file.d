package s3

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/minio/minio-go"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/file"
)

var (
	ErrCreateOutputPluginCantCheckBucket = errors.New("could not check bucket")
	ErrCreateOutputPluginNoSuchBucket    = errors.New("bucket doesn't exist")
)

type objStoreFactory func(cfg *Config) (ObjectStoreClient, map[string]ObjectStoreClient, error)

func (p *Plugin) minioClientsFactory(cfg *Config) (ObjectStoreClient, map[string]ObjectStoreClient, error) {
	minioClients := make(map[string]ObjectStoreClient)
	// initialize minio clients object for main bucket.
	defaultClient, err := minio.New(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Secure)
	if err != nil {
		return nil, nil, err
	}

	for i := range cfg.MultiBuckets {
		singleBucket := cfg.MultiBuckets[i]
		client, err := minio.New(singleBucket.Endpoint, singleBucket.AccessKey, singleBucket.SecretKey, singleBucket.Secure)
		if err != nil {
			return nil, nil, err
		}
		minioClients[singleBucket.Bucket] = client
	}

	minioClients[cfg.DefaultBucket] = defaultClient
	return defaultClient, minioClients, nil
}

func (p *Plugin) getStaticDirs(outPlugCount int) (map[string]string, error) {
	// dir for all bucket files.
	dir, _ := filepath.Split(p.config.FileConfig.TargetFile)

	targetDirs := make(map[string]string, outPlugCount)
	targetDirs[p.config.DefaultBucket] = dir
	// multi_buckets from config are sub dirs on in Config.FileConfig.TargetFile dir.
	for i := range p.config.MultiBuckets {
		singleBucket := &p.config.MultiBuckets[i]
		// todo bucket names can't intersect, add ability to have equal bucket names in different s3 servers.
		if _, ok := targetDirs[singleBucket.Bucket]; ok {
			return nil, fmt.Errorf("bucket name %s has duplicated", singleBucket.Bucket)
		}
		targetDirs[singleBucket.Bucket] = filepath.Join(dir, StaticBucketDir, singleBucket.Bucket) + dirSep
	}
	return targetDirs, nil
}

func (p *Plugin) getFileNames(outPlugCount int) map[string]string {
	fileNames := make(map[string]string, outPlugCount)

	_, f := filepath.Split(p.config.FileConfig.TargetFile)
	p.fileExtension = filepath.Ext(f)

	mainFileName := f[0 : len(f)-len(p.fileExtension)]
	fileNames[p.config.DefaultBucket] = mainFileName
	for i := range p.config.MultiBuckets {
		singleB := &p.config.MultiBuckets[i]
		fileNames[singleB.Bucket] = singleB.Bucket
	}
	return fileNames
}

// Try to create buckets from dirs lying in dynamic_dirs route
func (p *Plugin) createPlugsFromDynamicBucketArtifacts(targetDirs map[string]string) {
	type Test struct {
		A int
	}

	i := Test{4}
	_ = i
	dynamicDirsPath := filepath.Join(targetDirs[p.config.DefaultBucket], DynamicBucketDir)
	dynamicDir, err := os.ReadDir(dynamicDirsPath)
	if err != nil {
		p.logger.Infof("%s doesn't exist, won't restore dynamic s3 buckets", err.Error())
		return
	}
	for _, f := range dynamicDir {
		// dir name == bucket. Were interested only in dirs.
		if !f.IsDir() {
			continue
		}

		// If bucket was dynamic and now declared as static, we just ignore it.
		if p.config.IsMultiBucketExists(f.Name()) {
			continue
		}

		created := p.tryRunNewPlugin(f.Name())
		if created {
			p.logger.Infof("dynamic bucket %s retrieved from artifacts", f.Name())
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

func (p *Plugin) startPlugins(params *pipeline.OutputPluginParams, outPlugCount int, targetDirs, fileNames map[string]string) error {
	outPlugins := make(map[string]file.Plugable, outPlugCount)
	outPlugin, err := p.createOutPlugin(p.config.DefaultBucket)
	if err != nil {
		return err
	}
	outPlugins[p.config.DefaultBucket] = outPlugin
	p.logger.Infof("bucket %s exists", p.config.DefaultBucket)

	// If multi_buckets described on file.d config, check each of them as well.
	for i := range p.config.MultiBuckets {
		singleBucket := &p.config.MultiBuckets[i]
		outPlugin, err := p.createOutPlugin(singleBucket.Bucket)
		if err != nil {
			return err
		}
		outPlugins[singleBucket.Bucket] = outPlugin
		p.logger.Infof("bucket %s exists", singleBucket.Bucket)
	}

	p.logger.Info("outPlugins ready")
	p.outPlugins = file.NewFilePlugins(outPlugins)
	p.createPlugsFromDynamicBucketArtifacts(targetDirs)

	starterMap := make(pipeline.PluginsStarterMap, outPlugCount)
	for bucketName := range outPlugins {
		var starterData pipeline.PluginsStarterData
		// for defaultBucket set it's config.
		if bucketName == p.config.DefaultBucket {
			starterData = pipeline.PluginsStarterData{
				Config: &p.config.FileConfig,
				Params: params,
			}
		} else {
			// For multi_buckets copy main config and replace file path with bucket sub dir path.
			// Example /var/log/file.d.log => /var/log/static_bucket/bucketName/bucketName.log
			localBucketConfig := p.config.FileConfig
			localBucketConfig.TargetFile = fmt.Sprintf("%s%s%s", targetDirs[bucketName], fileNames[bucketName], p.fileExtension)
			starterData = pipeline.PluginsStarterData{
				Config: &localBucketConfig,
				Params: params,
			}
		}

		starterMap[bucketName] = starterData
	}
	p.outPlugins.Start(starterMap)

	return nil
}
