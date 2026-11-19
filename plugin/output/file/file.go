package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/gzip"

	"github.com/klauspost/compress/zstd"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/cfg/substitution"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/pierrec/lz4/v4"
	"go.uber.org/zap"
)

/*{ introduction
It sends event batches into files.
}*/

type Plugable interface {
	Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams)
	Out(event *pipeline.Event)
	Stop()
}

type Plugin struct {
	controller   pipeline.OutputPluginController
	logger       *zap.SugaredLogger
	config       *Config
	avgEventSize int
	batcher      *pipeline.Batcher
	cancel       context.CancelFunc
	openFiles    map[string]*activeFile
	staticPath   string // staticPath is target_file if it has no dynamic fields

	SealUpCallback func(string)

	mu *sync.RWMutex
}

type activeFile struct {
	file *os.File
	name string
	dir  string // dir + slash
	ext  string // dot + ext

	sealUpAt  time.Time // when the file is sealed up
	sealUpIdx int       // index in the name of the next sealed file
}

type data struct {
	outBuf  []byte
	pathOps []substitution.SubstitutionOp
	pathBuf []byte
	groups  map[string]*[]*pipeline.Event
}

const (
	outPluginType = "file"

	fileNameSeparator = "_"
)

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > File path to write events to.
	// > Until it's sealed up, the file is named with its creation unix time:
	// > `/var/log/file-d.log` -> `/var/log/1893445200_file-d.log`
	// >
	// > The path may contain `${field}` and `%y`, `%m`, `%d`.
	// > For example, if `target_file` is `/var/log/%y/%m/%d/${host}/file-d.log` file will be `/var/log/2006/01/02/host1/1893445200_file-d.log`
	TargetFile string `json:"target_file" default:"/var/log/file-d.log"` // *

	// > @3@4@5@6
	// >
	// > How long to write a file before sealing it up.
	RetentionInterval  cfg.Duration `json:"retention_interval" default:"1h" parse:"duration"` // *
	RetentionInterval_ time.Duration

	// > @3@4@5@6
	// >
	// > Compressed files format.
	Compression string `json:"compression" default:"none" options:"none|gzip|zstd|lz4"` // *

	// > @3@4@5@6
	// >
	// > Layout is added to targetFile after sealing up. Determines result file name
	TimeLayout string `json:"time_layout" default:"2006-01-02 15:04:05"` // *

	// > @3@4@5@6
	// >
	// > How much workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` // *
	WorkersCount_ int

	// > @3@4@5@6
	// >
	// > Maximum quantity of events to pack into one batch.
	BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4" parse:"expression"` // *
	BatchSize_ int

	// > @3@4@5@6
	// >
	// > A minimum size of events in a batch to send.
	// > If both batch_size and batch_size_bytes are set, they will work together.
	BatchSizeBytes  cfg.Expression `json:"batch_size_bytes" default:"0" parse:"expression"` // *
	BatchSizeBytes_ int

	// > @3@4@5@6
	// >
	// > After this timeout batch will be sent even if batch isn't completed.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"1s" parse:"duration"` // *
	BatchFlushTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > File mode for log files
	FileMode  cfg.Base8 `json:"file_mode" default:"0666" parse:"base8"` // *
	FileMode_ int64
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
	p.avgEventSize = params.PipelineSettings.AvgEventSize
	p.openFiles = make(map[string]*activeFile)

	pathOps, err := substitution.ParseSubstitution(p.config.TargetFile, nil, p.logger.Desugar())
	if err != nil {
		p.logger.Fatalf("can't parse target file: %s", err)
	}

	hasDynamicFields := false
	for _, op := range pathOps {
		if op.Kind == substitution.SubstitutionOpKindField {
			hasDynamicFields = true
		}
		if op.Kind == substitution.SubstitutionOpKindRaw && strings.Contains(op.Data[0], "%") {
			raw := op.Data[0]
			if strings.Count(raw, "%") != strings.Count(raw, "%y")+strings.Count(raw, "%m")+strings.Count(raw, "%d") {
				p.logger.Fatalf("only %%y, %%m, %%d are supported after %% in target_file: %s", p.config.TargetFile)
			}
			hasDynamicFields = true
		}
	}

	p.batcher = pipeline.NewBatcher(pipeline.BatcherOptions{
		PipelineName:   params.PipelineName,
		OutputType:     outPluginType,
		OutFn:          p.out,
		Controller:     p.controller,
		Workers:        p.config.WorkersCount_,
		BatchSizeCount: p.config.BatchSize_,
		BatchSizeBytes: p.config.BatchSizeBytes_,
		FlushTimeout:   p.config.BatchFlushTimeout_,
		MetricCtl:      params.MetricCtl,
	})

	p.mu = &sync.RWMutex{}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	if !hasDynamicFields {
		p.staticPath = string(p.buildTargetPath(nil, pathOps, nil))
		p.openFiles[p.staticPath] = p.openActive(p.staticPath)
	}

	go p.fileSealUpTicker(ctx)

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

// out writes the batch: into the single file or grouped by paths build from events.
func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) {
	if *workerData == nil {
		d := &data{outBuf: make([]byte, 0, p.config.BatchSize_*p.avgEventSize)}
		if p.staticPath == "" {
			d.pathOps, _ = substitution.ParseSubstitution(p.config.TargetFile, nil, p.logger.Desugar())
			d.pathBuf = make([]byte, 0, len(p.config.TargetFile))
			d.groups = make(map[string]*[]*pipeline.Event)
		}
		*workerData = d
	}
	data := (*workerData).(*data)

	// large batch can grow outBuf a lot, don't keep it
	if cap(data.outBuf) > p.config.BatchSize_*p.avgEventSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize_*p.avgEventSize)
	}

	if p.staticPath != "" {
		outBuf := data.outBuf[:0]
		batch.ForEach(func(event *pipeline.Event) {
			outBuf, _ = event.Encode(outBuf)
			outBuf = append(outBuf, byte('\n'))
		})
		data.outBuf = outBuf
		p.write(p.staticPath, outBuf)
		return
	}

	// groups events by paths
	batch.ForEach(func(event *pipeline.Event) {
		data.pathBuf = p.buildTargetPath(data.pathBuf[:0], data.pathOps, event)
		if events, ok := data.groups[string(data.pathBuf)]; ok {
			*events = append(*events, event)
			return
		}
		data.groups[string(data.pathBuf)] = &[]*pipeline.Event{event}
	})
	defer clear(data.groups)

	for path, events := range data.groups {
		outBuf := data.outBuf[:0]
		for _, event := range *events {
			outBuf, _ = event.Encode(outBuf)
			outBuf = append(outBuf, byte('\n'))
		}
		data.outBuf = outBuf
		p.write(path, outBuf)
	}
}

func (p *Plugin) fileSealUpTicker(ctx context.Context) {
	for {
		delay := p.config.RetentionInterval_
		p.mu.RLock()
		now := time.Now()
		for _, active := range p.openFiles {
			if until := active.sealUpAt.Sub(now); until < delay {
				delay = until
			}
		}
		p.mu.RUnlock()

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
			p.sealUpExpired(time.Now())
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (p *Plugin) write(path string, data []byte) {
	p.mu.RLock()
	if active, ok := p.openFiles[path]; ok {
		if _, err := active.file.Write(data); err != nil {
			p.logger.Fatalf("can't write into the file %s: %s", active.file.Name(), err)
		}
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	active, ok := p.openFiles[path]
	if !ok {
		active = p.openActive(path)
		p.openFiles[path] = active
	}
	if _, err := active.file.Write(data); err != nil {
		p.logger.Fatalf("can't write into the file %s: %s", active.file.Name(), err)
	}
}

func (p *Plugin) openActive(targetPath string) *activeFile {
	dir, name := filepath.Split(targetPath)
	ext := filepath.Ext(name)

	active := &activeFile{
		dir:  dir,
		name: name[:len(name)-len(ext)],
		ext:  ext,
	}

	if err := os.MkdirAll(active.dir, os.ModePerm); err != nil {
		p.logger.Fatalf("can't create target dir %s: %s", active.dir, err)
	}

	active.sealUpIdx = p.nextSealUpIdx(active)

	// reuse a current file after restart
	suffix := fileNameSeparator + active.name + active.ext
	matches, err := filepath.Glob(active.dir + "*" + suffix)
	if err != nil {
		p.logger.Panic(err)
	}
	if len(matches) > 0 {
		current := matches[0]
		ts, err := strconv.ParseInt(strings.TrimSuffix(filepath.Base(current), suffix), 10, 64)
		if err != nil {
			p.logger.Panicf("can't convert timestamp to int for file %s: %s", current, err)
		}

		file, err := os.OpenFile(current, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(p.config.FileMode_))
		if err != nil {
			p.logger.Panicf("can't create file %s: %s", current, err)
		}

		active.file = file
		active.sealUpAt = time.Unix(ts, 0).Add(p.config.RetentionInterval_)
		return active
	}

	p.createNew(active, time.Now())
	return active
}

func (p *Plugin) createNew(active *activeFile, now time.Time) {
	filePath := fmt.Sprintf("%s%d%s%s%s", active.dir, now.Unix(), fileNameSeparator, active.name, active.ext)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(p.config.FileMode_))
	if err != nil {
		p.logger.Panicf("can't create file %s: %s", filePath, err)
	}

	active.file = file
	active.sealUpAt = now.Add(p.config.RetentionInterval_)
}

func (p *Plugin) sealUpExpired(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for path, active := range p.openFiles {
		if active.sealUpAt.After(now) {
			continue
		}

		sealed := p.sealUp(active, now)

		if p.staticPath == "" {
			if !sealed {
				// empty file with dynamic path, so close and remove it, a new one is opened on the next write
				name := active.file.Name()
				if err := active.file.Close(); err != nil {
					p.logger.Panicf("can't close empty file %s: %s", name, err)
				}
				if err := os.Remove(name); err != nil {
					p.logger.Panicf("can't remove empty file %s: %s", name, err)
				}
			}
			delete(p.openFiles, path)
			continue
		}

		if sealed {
			p.createNew(active, now)
			continue
		}

		// empty file with static path: keep writing into it, check on the next tick
		active.sealUpAt = now.Add(p.config.RetentionInterval_)
	}
}

// sealUp closes and renames the file: app.log -> app_<idx>_<time>.log, returns false for an empty file.
func (p *Plugin) sealUp(active *activeFile, now time.Time) bool {
	info, err := active.file.Stat()
	if err != nil {
		p.logger.Panicf("can't get info about file %s: %s", active.file.Name(), err)
	}
	if info.Size() == 0 {
		return false
	}

	oldPath := active.file.Name()
	if err := active.file.Close(); err != nil {
		p.logger.Panicf("can't close file %s: %s", active.file.Name(), err)
	}

	name := fmt.Sprintf("%s%s%d%s%s%s", active.name, fileNameSeparator, active.sealUpIdx, fileNameSeparator, now.Format(p.config.TimeLayout), active.ext)
	newFilePath := filepath.Join(active.dir, name)
	if err := os.Rename(oldPath, newFilePath); err != nil {
		p.logger.Panicf("can't rename %s to %s: %s", oldPath, newFilePath, err)
	}
	active.sealUpIdx++

	p.logger.Infof("sealing file, newFilePath=%s", newFilePath)

	go func(path string) {
		pathAfterCompress := p.compress(path)
		if p.SealUpCallback != nil {
			go p.SealUpCallback(pathAfterCompress)
		}
	}(newFilePath)

	return true
}

// nextSealUpIdx returns the index for the next sealed file.
func (p *Plugin) nextSealUpIdx(active *activeFile) int {
	// sealed up files can be compressed
	pattern := fmt.Sprintf("%s%s%s*%s*%s*", active.dir, active.name, fileNameSeparator, fileNameSeparator, active.ext)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		p.logger.Panic(err)
	}

	idx := -1
	for _, filePath := range matches {
		file := filepath.Base(filePath)
		i, _, _ := strings.Cut(file[len(active.name)+len(fileNameSeparator):], fileNameSeparator)
		maxIdx, err := strconv.Atoi(i)
		if err != nil {
			break
		}
		if maxIdx > idx {
			idx = maxIdx
		}
	}

	return idx + 1
}

var dateLayouts = map[byte]string{'y': "2006", 'm': "01", 'd': "02"}

// buildTargetPath builds the file path for the event from target_file.
func (p *Plugin) buildTargetPath(dst []byte, pathOps []substitution.SubstitutionOp, event *pipeline.Event) []byte {
	var ts time.Time
	for _, op := range pathOps {
		switch op.Kind {
		case substitution.SubstitutionOpKindRaw:
			raw := op.Data[0]
			if ts.IsZero() && strings.Contains(raw, "%") {
				for _, field := range [...]string{"timestamp", "time", "ts"} {
					if node := event.Root.Dig(field).AsString(); node != "" {
						ts, _ = time.Parse(p.config.TimeLayout, node)
						break
					}
				}
			}
			if len(dst) > 0 && dst[len(dst)-1] == '/' && strings.HasPrefix(raw, "/") {
				raw = raw[1:]
			}

			for {
				before, after, ok := strings.Cut(raw, "%")
				dst = append(dst, before...)
				if !ok {
					break
				}
				dst = ts.AppendFormat(dst, dateLayouts[after[0]])
				raw = after[1:]
			}
		case substitution.SubstitutionOpKindField:
			node := event.Root.Dig(op.Data...)
			// pass only bool, str, num types
			if node.IsNil() || !(node.IsString() || node.IsNumber() || node.IsTrue() || node.IsFalse()) {
				continue
			}

			start := len(dst)
			dst = append(dst, node.AsString()...)

			// filters change tha value in place, so it's applied to its copy in dst
			value := dst[start:]
			for _, filter := range op.Filters {
				value = filter.Apply(value, value)
			}
			dst = append(dst[:start], value...)
		}
	}

	return dst
}

func (p *Plugin) compress(path string) string {
	var newWriter func(io.Writer) (io.WriteCloser, error)
	var ext string

	switch p.config.Compression {
	case "none":
		return path
	case "gzip":
		ext = ".gz"
		newWriter = func(w io.Writer) (io.WriteCloser, error) {
			return gzip.NewWriter(w), nil
		}
	case "zstd":
		ext = ".zst"
		newWriter = func(w io.Writer) (io.WriteCloser, error) {
			return zstd.NewWriter(w)
		}
	case "lz4":
		ext = ".lz4"
		newWriter = func(w io.Writer) (io.WriteCloser, error) {
			return lz4.NewWriter(w), nil
		}
	}

	src, err := os.Open(path)
	if err != nil {
		p.logger.Errorf("can't open file to compress %s: %s", path, err)
		return path
	}

	compPath := path + ext
	dst, err := os.OpenFile(compPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(p.config.FileMode_))
	if err != nil {
		_ = src.Close()
		p.logger.Errorf("can't create file %s: %s", compPath, err)
		return path
	}

	w, err := newWriter(dst)
	if err == nil {
		_, err = io.Copy(w, src)
		err = errors.Join(err, w.Close())
	}

	err = errors.Join(err, dst.Close(), src.Close())
	if err != nil {
		err = errors.Join(err, os.Remove(compPath))
		p.logger.Errorf("can't compress file %s: %s", path, err)
		return path
	}

	if err := os.Remove(path); err != nil {
		p.logger.Errorf("can't remove file %s: %s", path, err)
	}

	return compPath
}
