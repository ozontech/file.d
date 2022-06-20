package file

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/longpanic"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type Plugable interface {
	Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams)
	Out(event *pipeline.Event)
	Stop()
}

type Plugin struct {
	controller     pipeline.OutputPluginController
	logger         *zap.SugaredLogger
	config         *Config
	avgEventSize   int
	batcher        *pipeline.Batcher
	file           *os.File
	cancel         context.CancelFunc
	idx            int
	nextSealUpTime time.Time

	targetDir     string
	fileExtension string
	fileName      string
	tsFileName    string

	SealUpCallback   func(string, string)
	FileMetaCallback func(filename string) (fileOuterDestination, ext string)

	mu *sync.RWMutex

	timestampFileFormat string
	metaWriter          *meta
	pairOfTimestamps    *pair
}

type data struct {
	outBuf []byte
}

const (
	outPluginType = "file"

	fileNameSeparator = "_"
)

type Config struct {
	//> File name for log file.
	//> defaultTargetFileName = TargetFile default value
	TargetFile string `json:"target_file" default:"/var/log/file-d.log"` //*

	//> Interval of creation new file
	RetentionInterval  cfg.Duration `json:"retention_interval" default:"1h" parse:"duration"` //*
	RetentionInterval_ time.Duration

	//> Layout is added to targetFile after sealing up. Determines result file name
	Layout string `json:"time_layout" default:"01-02-2006_15:04:05"` //*

	//> How much workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` //*
	WorkersCount_ int

	//> Maximum quantity of events to pack into one batch.
	BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4" parse:"expression"` //*
	BatchSize_ int

	//> After this timeout batch will be sent even if batch isn't completed.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"1s" parse:"duration"` //*
	BatchFlushTimeout_ time.Duration

	//> File mode for log files
	FileMode  cfg.Base8 `json:"file_mode" default:"0666" parse:"base8"` //*
	FileMode_ int64

	//> MetaCfg describes metafile with sealfile desription
	MetaCfg MetaConfig `json:"meta_config" child:"true"`
}

type MetaConfig struct {
	//> Stores metadata of sealed files.
	StoreMeta bool `json:"store_meta"` //*
	//> MetaDataFile saves metainformation about sealed file.
	MetaDataDir string `json:"metadata_dir" default:"/var/log/file-d.meta"` //*
	//> SealedMetaPrefix defines prefix of sealed meta file
	SealedMetaPrefix string `json:"sealed_metafile_prefix" default:"sealed_meta_"` //*
	//> Which field of the event should be used as `timestamp` GELF field.
	TimestampField string `json:"timestamp_field" default:"time"` //*
	//> In which format timestamp field should be parsed.
	TimestampFieldFormat string `json:"timestamp_field_format" default:"rfc3339nano" options:"ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano"` //*
	//> Static part of json meta.
	StaticMeta string `json:"static_meta" default:""` //*
	//> SealedFileNameField stores name of sead file
	SealedFileNameField string `json:"sealed_filename_field" default:"sealed_name"` //*
	//> SealedFilePathFieldName stores name of sead file with path
	SealedFilePathFieldName string `json:"sealed_filepath_name_field" default:"sealed_path"` //*
}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    outPluginType,
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

	dir, file := filepath.Split(p.config.TargetFile)
	p.targetDir = dir
	p.fileExtension = filepath.Ext(file)
	p.fileName = file[0 : len(file)-len(p.fileExtension)]
	p.tsFileName = "%s" + "-" + p.fileName

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	if p.config.MetaCfg.StoreMeta {
		metaWriter, err := newMeta(
			metaInit{
				fileName:                p.fileName,
				separator:               fileNameSeparator,
				dir:                     p.config.MetaCfg.MetaDataDir,
				extention:               "json",
				filePrefix:              metaFilePrefix,
				sealedPrefix:            p.config.MetaCfg.SealedMetaPrefix,
				sealedFileFieldName:     p.config.MetaCfg.SealedFileNameField,
				sealedFilePathFieldName: p.config.MetaCfg.SealedFilePathFieldName,
				fileMode:                p.config.FileMode_,
				staticMeta:              p.config.MetaCfg.StaticMeta,
			},
		)
		if err != nil {
			params.Logger.Fatalf("can't create meta: %s", err.Error())
		}

		p.metaWriter = metaWriter

		format, err := pipeline.ParseFormatName(p.config.MetaCfg.TimestampFieldFormat)
		if err != nil {
			params.Logger.Fatalf("unknown time format: %s", err.Error())
		}
		p.timestampFileFormat = format
		p.pairOfTimestamps = NewPair()
	}

	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		outPluginType,
		p.out,
		nil,
		p.controller,
		p.config.WorkersCount_,
		p.config.BatchSize_,
		p.config.BatchFlushTimeout_,
		0,
	)

	p.mu = &sync.RWMutex{}

	if err := os.MkdirAll(p.targetDir, os.ModePerm); err != nil {
		p.logger.Fatalf("could not create target dir: %s, error: %s", p.targetDir, err.Error())
	}

	p.idx = p.getStartIdx()
	p.startUpCreateNewFile()
	p.setNextSealUpTime()

	if p.config.MetaCfg.StoreMeta {
		longpanic.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(p.config.RetentionInterval_ / 10):
					p.mu.RLock()
					min, max := p.pairOfTimestamps.Get()
					p.mu.RUnlock()

					if err := p.metaWriter.updateMetaFileWithLock(min, max); err != nil {
						p.logger.Errorf("can't update meta: %s", err.Error())
					}
				}
			}
		})
	}

	if p.file == nil {
		p.logger.Panic("file struct is nil!")
	}
	if p.nextSealUpTime.IsZero() {
		p.logger.Panic("next seal up time is nil!")
	}

	longpanic.Go(func() {
		p.fileSealUpTicker(ctx)
	})
	p.batcher.Start(ctx)
}

func (p *Plugin) Stop() {
	// we MUST NOT close file, through p.file.Close(), fileSealUpTicker already do this duty.
	p.batcher.Stop()
	p.cancel()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) {
	if *workerData == nil {
		*workerData = &data{
			outBuf: make([]byte, 0, p.config.BatchSize_*p.avgEventSize),
		}
	}
	data := (*workerData).(*data)

	// handle to much memory consumption
	if cap(data.outBuf) > p.config.BatchSize_*p.avgEventSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize_*p.avgEventSize)
	}

	outBuf := data.outBuf[:0]

	minTs, maxTs := int64(0), int64(0)
	for _, event := range batch.Events {
		outBuf, _ = event.Encode(outBuf)
		outBuf = append(outBuf, byte('\n'))

		if p.config.MetaCfg.StoreMeta {
			ts, err := event.Root.DigStrict("timestamp")
			if err != nil {
				p.logger.Errorf("doesn't have timestamp field: %s", err.Error())
				continue
			}
			tsInt, err := ts.AsInt64()
			if err != nil {
				p.logger.Errorf("fime field isn't int64: %s", err.Error())
				continue
			}
			if tsInt < minTs || minTs == 0 {
				minTs = tsInt
			}
			if tsInt > maxTs {
				maxTs = tsInt
			}
		}
	}
	data.outBuf = outBuf

	if p.config.MetaCfg.StoreMeta {
		p.mu.Lock()
		p.pairOfTimestamps.UpdatePair(minTs, maxTs)
		p.mu.Unlock()
	}

	p.write(outBuf)
}

func (p *Plugin) fileSealUpTicker(ctx context.Context) {
	for {
		timer := time.NewTimer(time.Until(p.nextSealUpTime))
		select {
		case <-timer.C:
			p.sealUp()
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (p *Plugin) setNextSealUpTime() {
	ts := p.tsFileName[0 : len(p.tsFileName)-len(fileNameSeparator)-len(p.fileName)-len(p.fileExtension)]
	t, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		p.logger.Panicf("coult nod convert timestamp to int for file: %s, error: %s", p.tsFileName, err.Error())
	}
	creationTime := time.Unix(t, 0)
	p.nextSealUpTime = creationTime.Add(p.config.RetentionInterval_)
}

func (p *Plugin) write(data []byte) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if _, err := p.file.Write(data); err != nil {
		p.logger.Fatalf("could not write into the file: %s, error: %s", p.file.Name(), err.Error())
	}
}

// startUpCreateNewFile creates of restores log and meta files during startUp
func (p *Plugin) startUpCreateNewFile() {
	p.createNew("", "")
}

// createNew creates new or appends to existing log file
func (p *Plugin) createNew(sealingLogFile, sealingOuterPath string) {
	timestamp := time.Now().Unix()
	// file name like: 1654785468_file-d.log
	p.tsFileName = fmt.Sprintf("%d%s%s%s", timestamp, fileNameSeparator, p.fileName, p.fileExtension)
	logger.Infof("tsFileName in createNew=%s", p.tsFileName)
	// full path like: /var/log/1654785468_file-d.log
	fileName := fmt.Sprintf("%s%s", p.targetDir, p.tsFileName)
	// search for old unsealed file
	pattern := fmt.Sprintf("%s*%s%s%s", p.targetDir, fileNameSeparator, p.fileName, p.fileExtension)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		p.logger.Fatalf("can't glob: pattern=%s, err=%ss", pattern, err.Error())
	}
	// existense of file means crash of prev run before it was sealed. Pull up old file instead of new
	if len(matches) == 1 {
		p.tsFileName = path.Base(matches[0])
		fileName = fmt.Sprintf("%s%s", p.targetDir, p.tsFileName)
	}
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(p.config.FileMode_))
	if err != nil {
		p.logger.Panicf("could not open or create file: %s, error: %s", fileName, err.Error())
	}
	p.file = file

	// new meta file
	if p.config.MetaCfg.StoreMeta {
		// createNew() guarded by mutex on upper layer.
		f, l := p.pairOfTimestamps.Reset()
		if err := p.metaWriter.newMetaFile(
			p.fileName,
			sealUpDTO{
				sealingOuterPath: sealingOuterPath,
				sealingLogFile:   sealingLogFile,
				firstTimestamp:   f,
				lastTimestamp:    l,
			},
			timestamp,
		); err != nil {
			p.logger.Panic(err)
		}
	}
}

// sealUp manages current file: renames, closes, and creates new.
func (p *Plugin) sealUp() {
	info, err := p.file.Stat()
	if err != nil {
		p.logger.Panicf("could not get info about file: %s, error: %s", p.file.Name(), err.Error())
	}
	if info.Size() == 0 {
		return
	}

	// newFileName will be like: ".var/log/log_1_01-02-2009_15:04.log
	now := time.Now().Format(p.config.Layout)
	finalOldFileName := filepath.Join(p.targetDir, fmt.Sprintf("%s%s%d%s%s%s", p.fileName, fileNameSeparator, p.idx, fileNameSeparator, now, p.fileExtension))
	p.rename(finalOldFileName)
	oldFile := p.file
	p.mu.Lock()

	outerPath, ext := "", ""
	if p.FileMetaCallback != nil {
		outerPath, ext = p.FileMetaCallback(filepath.Base(finalOldFileName))
	}

	p.createNew(filepath.Base(finalOldFileName)+ext, outerPath)
	p.nextSealUpTime = time.Now().Add(p.config.RetentionInterval_)
	p.mu.Unlock()
	if err := oldFile.Close(); err != nil {
		p.logger.Panicf("could not close file: %s, error: %s", oldFile.Name(), err.Error())
	}

	logger.Errorf("sealing in %d, newFile: %s", time.Now().Unix(), finalOldFileName)
	if p.SealUpCallback != nil {
		if outerPath != "" {
			outerPath = filepath.Base(outerPath)
		}
		longpanic.Go(func() { p.SealUpCallback(finalOldFileName, outerPath) })
	}
}

func (p *Plugin) rename(newFileName string) {
	f := fmt.Sprintf("%s%s", p.targetDir, p.tsFileName)
	if err := os.Rename(f, newFileName); err != nil {
		p.logger.Panicf("could not rename file, error: %s", err.Error())
	}
	p.idx++
}

func (p *Plugin) getStartIdx() int {
	pattern := fmt.Sprintf("%s/%s%s*%s*%s", p.targetDir, p.fileName, fileNameSeparator, fileNameSeparator, p.fileExtension)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		p.logger.Panic(err.Error())
	}
	idx := -1
	for _, v := range matches {
		file := filepath.Base(v)
		i := file[len(p.fileName)+len(fileNameSeparator) : len(file)-len(p.fileExtension)-len(p.config.Layout)-len(fileNameSeparator)]
		maxIdx, err := strconv.Atoi(i)
		if err != nil {
			break
		}
		if maxIdx > idx {
			idx = maxIdx
		}
	}
	idx++
	return idx
}
