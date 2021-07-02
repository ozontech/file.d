package file

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"

	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type Plugin struct {
	controller     pipeline.OutputPluginController
	logger         *zap.SugaredLogger
	config         *Config
	avgLogSize     int
	batcher        *pipeline.Batcher
	file           *os.File
	ctx            context.Context
	cancelFunc     context.CancelFunc
	idx            int
	nextSealUpTime time.Time

	mu *sync.RWMutex
}

type data struct {
	outBuf []byte
}

const (
	fileNameSeparator  = "_"
	fileSealUpInterval = time.Second
)

type Config struct {
	//> File name for log file.
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
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"1s"` //*
	BatchFlushTimeout_ time.Duration

	//> File mode for log files
	FileMode  cfg.Base8 `json:"file_mode" default:"0666" parse:"base8"` //*
	FileMode_ int64

	TargetDir     string
	FileExtension string
	FileName      string
}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    "file",
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

	p.config.TargetDir = dir
	p.config.FileExtension = filepath.Ext(file)
	p.config.FileName = file[0 : len(file)-len(p.config.FileExtension)]

	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		"file",
		p.out,
		nil,
		p.controller,
		p.config.WorkersCount_,
		p.config.BatchSize_,
		p.config.BatchFlushTimeout_,
		0,
	)
	p.mu = &sync.RWMutex{}

	ctx, cancel := context.WithCancel(context.Background())
	p.ctx = ctx
	p.cancelFunc = cancel
	//create target dir
	if _, err := os.Stat(p.config.TargetDir); os.IsNotExist(err) {
		if err := os.MkdirAll(p.config.TargetDir, os.ModePerm); err != nil {
			p.logger.Fatalf("could not create target dir: %s, error: %s", p.config.TargetDir, err.Error())
		}
	}
	p.idx = p.getStartIdx()
	// seal up old file if have it
	if p.shouldSealUp() {
		p.sealUp()
	} else {
		p.file = p.createNew()
	}

	if p.file == nil {
		p.logger.Panic("file struct is nil!")
	}
	if p.nextSealUpTime.IsZero() {
		p.nextSealUpTime = time.Now().Add(p.config.RetentionInterval_)
	}

	go p.fileSealUpTicker()
	p.batcher.Start()
}

func (p *Plugin) Stop() {
	p.cancelFunc()
	p.batcher.Stop()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) {
	if *workerData == nil {
		*workerData = &data{
			outBuf: make([]byte, 0, p.config.BatchSize_*p.avgLogSize),
		}
	}
	data := (*workerData).(*data)

	// handle to much memory consumption
	if cap(data.outBuf) > p.config.BatchSize_*p.avgLogSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize_*p.avgLogSize)
	}

	outBuf := data.outBuf[:0]

	for _, event := range batch.Events {
		outBuf, _ = event.Encode(outBuf)
		outBuf = append(outBuf, byte('\n'))
	}
	data.outBuf = outBuf

	p.write(outBuf)
}

func (p *Plugin) fileSealUpTicker() {
	ticker := time.NewTicker(fileSealUpInterval)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			if t.After(p.nextSealUpTime) {
				p.sealUp()
			}
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Plugin) shouldSealUp() bool {
	info, err := os.Stat(p.config.TargetFile)
	if err != nil {
		return false
	}
	stat_t := info.Sys().(*syscall.Stat_t)
	creationTime := time.Unix(stat_t.Birthtimespec.Sec, stat_t.Birthtimespec.Nsec)
	currentSealUpTime := creationTime.Add(p.config.RetentionInterval_)
	p.nextSealUpTime = currentSealUpTime
	isOld := time.Now().After(currentSealUpTime)
	return info.Size() > 0 && isOld
}

func (p *Plugin) write(data []byte) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if _, err := p.file.Write(data); err != nil {
		p.logger.Fatalf("could not write into the file: %s, error: %s", p.file.Name(), err.Error())
	}
}

func (p *Plugin) createNew() *os.File {
	file, err := os.OpenFile(p.config.TargetFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(p.config.FileMode_))
	if err != nil {
		p.logger.Panicf("could not open or create file: %s, error: %s", p.config.TargetFile, err.Error())
	}
	return file
}

//sealUp manege current log file. Renames it, close it and create new log file.
func (p *Plugin) sealUp() {
	// if size file is zero, we do not seal up the file
	if p.file != nil {
		info, err := p.file.Stat()
		if err != nil {
			p.logger.Panicf("could not get info about file: %s, error: %s", p.file.Name(), err.Error())
		}
		if info.Size() == 0 {
			return
		}
	}
	p.rename()
	oldFile := p.file
	p.mu.Lock()
	p.file = p.createNew()
	p.nextSealUpTime = time.Now().Add(p.config.RetentionInterval_)
	p.mu.Unlock()
	if oldFile != nil {
		if err := oldFile.Close(); err != nil {
			p.logger.Panicf("could not close file: %s, error: %s", oldFile.Name(), err.Error())
		}
	}

}

func (p *Plugin) rename() {
	fullFileName := filepath.Join(p.config.TargetDir, fmt.Sprintf("%s%s%d%s%s%s", p.config.FileName, fileNameSeparator, p.idx, fileNameSeparator, time.Now().Format(p.config.Layout), p.config.FileExtension))
	if err := os.Rename(p.config.TargetFile, fullFileName); err != nil {
		p.logger.Panicf("could not rename file, error: %s", err.Error())
	}
	p.idx++
}

func (p *Plugin) getStartIdx() int {
	pattern := fmt.Sprintf("%s/%s%s*%s*%s", p.config.TargetDir, p.config.FileName, fileNameSeparator, fileNameSeparator, p.config.FileExtension)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		p.logger.Panic(err.Error())
	}
	idx := -1
	for _, v := range matches {
		file := filepath.Base(v)
		i := file[len(p.config.FileName)+len(fileNameSeparator) : len(file)-len(p.config.FileExtension)-len(p.config.Layout)-len(fileNameSeparator)]
		maxIdx, err := strconv.Atoi(i)
		if err != nil {
			p.logger.Panicf("wrong number for file: %s,  error: %s", file, err.Error())
		}
		if maxIdx > idx {
			idx = maxIdx
		}
	}
	idx++
	return idx
}
