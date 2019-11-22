package file

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/output/devnull"
	"go.uber.org/atomic"
)

var (
	filesDir   = ""
	offsetsDir = ""
)

const offsetsFile = "filed_offsets.yaml"

func setup() {
	f, err := ioutil.TempDir("", "input_file")
	if err != nil {
		panic(err.Error())
	}
	filesDir = f

	f, err = ioutil.TempDir("", "input_file_offsets")
	if err != nil {
		panic(err.Error())
	}
	offsetsDir = f
}

func shutdown() {
	err := os.RemoveAll(filesDir)
	if err != nil {
		panic(err.Error())
	}
}

func startPipeline(persistenceMode string, enableEventLog bool, config *Config) (*pipeline.Pipeline, *Plugin) {
	p := pipeline.NewTestPipeLine(true)
	if enableEventLog {
		p.EnableEventLog()
	}

	if config == nil {
		config = &Config{WatchingDir: filesDir, OffsetsFile: filepath.Join(offsetsDir, offsetsFile), PersistenceMode: persistenceMode}
	}
	anyPlugin, _ := Factory()
	inputPlugin := anyPlugin.(*Plugin)
	inputPlugin.disableFinalSave()
	p.SetInputPlugin(&pipeline.InputPluginData{Plugin: inputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutputPlugin(&pipeline.OutputPluginData{Plugin: outputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	p.Start()

	return p, inputPlugin
}

func renameFile(oldFile string, newFile string) {
	err := os.Rename(oldFile, newFile)
	if err != nil {
		panic(err.Error())
	}
}

func truncateFile(file string) {
	f, err := os.OpenFile(file, os.O_WRONLY, 0664)
	if err != nil {
		panic(err.Error())
	}
	defer func() {
		err := f.Close()
		if err != nil {
			panic(err.Error())
		}
	}()

	err = f.Truncate(0)
	if err != nil {
		panic(err.Error())
	}
}

func rotateFile(file string) string {
	newFile := file + ".new"
	renameFile(file, newFile)
	createFile(file)

	return newFile
}

func createFile(file string) {
	fd, err := os.Create(file)
	if err != nil {
		panic(err.Error())
	}
	err = fd.Close()
	if err != nil {
		panic(err.Error())
	}
}

func createTempFile() string {
	u := uuid.NewV4().String()
	file, err := os.Create(path.Join(filesDir, u))
	if err != nil {
		panic(err.Error())
	}

	return file.Name()
}

func createOffsetFile() string {
	file, err := os.Create(path.Join(offsetsDir, offsetsFile))
	if err != nil {
		panic(err.Error())
	}

	return file.Name()
}

func addDataFile(file *os.File, data []byte) {
	if _, err := file.Write(data);
		err != nil {
		panic(err.Error())
	}
}

func addData(file string, data []byte, isLine bool, sync bool) {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0660)
	if err != nil {
		panic(err.Error())
	}
	defer func() {
		err := f.Close()
		if err != nil {
			panic(err.Error())
		}
	}()

	if _, err = f.Write(data); err != nil {
		panic(err.Error())
	}
	if isLine {
		if _, err = f.Write([]byte{'\n'}); err != nil {
			panic(err.Error())
		}
	}

	if sync {
		err = f.Sync()
		if err != nil {
			panic(err.Error())
		}
	}
}
func addLines(file string, from int, to int) {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0660)
	if err != nil {
		panic(err.Error())
	}

	defer func() {
		err := f.Close()
		if err != nil {
			panic(err.Error())
		}
	}()

	for i := from; i < to; i++ {

		if _, err = f.WriteString(fmt.Sprintf(`"_`)); err != nil {
			panic(err.Error())
		}
		if _, err = f.WriteString(fmt.Sprintf(`%d"`+"\n", i)); err != nil {
			panic(err.Error())
		}
	}

	err = f.Sync()
	if err != nil {
		panic(err.Error())
	}
}

func getContent(file string) string {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}

	return string(content)
}

func getContentBytes(file string) []byte {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}

	return content
}

func genOffsetsContent(file string, offset int) string {
	return fmt.Sprintf(`- file: %d %s
  not_set: %d
`, getInodeByFile(file), file, offset)
}

func genOffsetsContentMultiple(files []string, offset int) string {
	result := make([]byte, 0, len(files)*100)
	for _, file := range files {
		result = append(result, fmt.Sprintf(`- file: %d %s
  not_set: %d
`, getInodeByFile(file), file, offset)...)
	}

	return string(result)
}

func genOffsetsContentMultipleStreams(files []string, offsetStdErr int, offsetStdOut int) string {
	result := make([]byte, 0, len(files)*100)
	for _, file := range files {
		result = append(result, fmt.Sprintf(`- file: %d %s
  stderr: %d
  stdout: %d
`, getInodeByFile(file), file, offsetStdErr, offsetStdOut)...)
	}

	return string(result)
}

func getInodeByFile(file string) uint64 {
	stat, err := os.Stat(file)
	if err != nil {
		panic(err)
	}
	sysStat := stat.Sys().(*syscall.Stat_t)
	inode := sysStat.Ino
	return inode
}

func assertOffsetsEqual(t *testing.T, offsetsContentA string, offsetsContentB string) {
	offsetsA := parseOffsets(offsetsContentA)
	offsetsB := parseOffsets(offsetsContentB)
	for sourceID, inode := range offsetsA {
		_, has := offsetsB[sourceID]
		assert.True(t, has, "offsets aren't equal, source id=%d", sourceID)
		for stream, offset := range inode.streams {
			offsetB, has := offsetsB[sourceID].streams[stream]
			assert.True(t, has, "offsets aren't equal, no stream %q", stream)
			assert.Equal(t, offset, offsetB, "offsets aren't equal")
		}
	}
}

func TestWatch(t *testing.T) {
	setup()
	defer shutdown()

	c, _ := startPipeline("async", true, nil)
	defer c.Stop()

	file := createTempFile()
	content := []byte(`{"key":"value"}` + "\n")

	time.Sleep(time.Millisecond * 500)
	dir := filepath.Join(filepath.Dir(file), "dir1")
	_ = os.Mkdir(dir, 0770)
	time.Sleep(time.Millisecond * 500)

	err := ioutil.WriteFile(filepath.Join(dir, "new_file"), content, 0660)
	if err != nil {
		panic(err.Error())
	}
	err = ioutil.WriteFile(filepath.Join(dir, "other_file"), content, 0660)
	if err != nil {
		panic(err.Error())
	}
	time.Sleep(time.Millisecond * 500)
	err = os.RemoveAll(dir)
	if err != nil {
		panic(err.Error())
	}

	dir = filepath.Join(filepath.Dir(file), "dir2")
	_ = os.Mkdir(dir, 0770)
	time.Sleep(time.Millisecond * 500)

	err = ioutil.WriteFile(filepath.Join(dir, "new_file"), content, 0660)
	if err != nil {
		panic(err.Error())
	}
	err = ioutil.WriteFile(filepath.Join(dir, "other_file"), content, 0660)
	if err != nil {
		panic(err.Error())
	}
	time.Sleep(time.Millisecond * 500)
	err = os.RemoveAll(dir)
	if err != nil {
		panic(err.Error())
	}

	addData(file, content, true, true)
	c.HandleEventFlowFinish(false)

	assert.Equal(t, 5, c.GetEventLogLength(), "wrong log count")
	assert.Equal(t, 5, c.GetEventsTotal(), "wrong log count")
}

func TestReadLineSimple(t *testing.T) {
	setup()
	defer shutdown()

	c, _ := startPipeline("async", true, nil)
	defer c.Stop()

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1"}`), true, true)
	addData(file, []byte(`{"Data":"Line2"}`), true, true)
	addData(file, []byte(`{"Data":"Line3"}`), true, true)
	c.HandleEventFlowFinish(false)

	c.WaitUntilDone(false)

	assert.Equal(t, 3, c.GetEventLogLength(), "wrong log count")
	assert.Equal(t, 3, c.GetEventsTotal(), "wrong log count")
	assert.Equal(t, `{"Data":"Line1"}`, c.GetEventLogItem(0), "Wrong log")
	assert.Equal(t, `{"Data":"Line2"}`, c.GetEventLogItem(1), "Wrong log")
	assert.Equal(t, `{"Data":"Line3"}`, c.GetEventLogItem(2), "Wrong log")

}

func TestOffsetsSimple(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1"}`), true, false)
	addData(file, []byte(`{"Data":"Line2"}`), true, false)
	addData(file, []byte(`{"Data":"Line3"}`), true, false)
	c.HandleEventFlowFinish(false)

	c.WaitUntilDone(false)

	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, 51), getContent(p.config.OffsetsFile), "wrong offsets")
}

func TestOffsetsStart(t *testing.T) {
	setup()
	defer shutdown()

	file := createTempFile()
	addData(file, []byte(`{"Data":`), false, false)

	config := &Config{WatchingDir: filesDir, OffsetsFile: filepath.Join(offsetsDir, offsetsFile), PersistenceMode: "sync", OffsetsOp: "tail"}
	c, p := startPipeline("async", true, config)
	defer c.Stop()

	addData(file, []byte(`"Line1"}`), true, false)
	addData(file, []byte(`{"Data":"Line2"}`), true, false)

	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, 34), getContent(p.config.OffsetsFile), "wrong offsets")
}

func TestLoadOffsets(t *testing.T) {
	setup()
	defer shutdown()

	data := `{"some key":"some data"}`
	dataFile := createTempFile()
	addData(dataFile, []byte(data), false, false)

	offsetFile := createOffsetFile()
	offsets := genOffsetsContent(dataFile, len(data))
	addData(offsetFile, []byte(offsets), false, false)

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	p.config.OffsetsFile += ".new"
	p.jobProvider.saveOffsets()
	assert.Equal(t, offsets, getContent(p.config.OffsetsFile), "wrong offsets")
}

func TestContinueReading(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startPipeline("async", true, nil)

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1"}`), true, false)
	addData(file, []byte(`{"Data":"Line2"}`), true, false)
	addData(file, []byte(`{"Data":"Line3"}`), true, false)
	c.HandleEventFlowFinish(false)

	c.WaitUntilDone(false)
	c.Stop()
	p.jobProvider.saveOffsets()
	processed := c.GetEventsTotal()

	addData(file, []byte(`{"Data":"Line4"}`), true, false)
	addData(file, []byte(`{"Data":"Line5"}`), true, false)
	addData(file, []byte(`{"Data":"Line6"}`), true, false)
	addData(file, []byte(`{"Data":"Line7"}`), true, false)

	c, p = startPipeline("async", true, nil)

	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)
	c.Stop()

	assert.Equal(t, 7, processed+c.GetEventsTotal(), "wrong log count")
	assert.Equal(t, `{"Data":"Line7"}`, c.GetEventLogItem(c.GetEventLogLength()-1), "Wrong log")
	assert.Equal(t, genOffsetsContent(file, 119), getContent(p.config.OffsetsFile), "wrong offsets")
}

func TestReadSeq(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	file := createTempFile()

	addData(file, []byte(`{"Data":"Line1`), false, true)
	addData(file, []byte(`Line2`), false, true)
	addData(file, []byte(`Line3"}`), true, true)
	c.HandleEventFlowFinish(false)

	c.WaitUntilDone(false)

	assert.Equal(t, 1, c.GetEventLogLength(), "wrong log count")
	assert.Equal(t, 1, c.GetEventsTotal(), "wrong log count")
	assert.Equal(t, `{"Data":"Line1Line2Line3"}`, c.GetEventLogItem(0), "Wrong log")
	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, 27), getContent(p.config.OffsetsFile), "wrong offsets")
}

func TestReadLong(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	file := createTempFile()

	s := ""
	overhead := 100
	for i := 0; i < defaultReadBufferSize+overhead; i++ {
		s = s + "a"
	}
	data := fmt.Sprintf(`{"Data":"%s"}`+"\n"+`{"Data":"xxx"}`, s)

	addData(file, []byte(data), true, false)
	addData(file, []byte(data), true, false)
	addData(file, []byte(data), true, false)
	c.HandleEventFlowFinish(false)

	c.WaitUntilDone(false)

	assert.Equal(t, 6, c.GetEventLogLength(), "wrong log count")
	assert.Equal(t, 6, c.GetEventsTotal(), "wrong log count")
	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, (defaultReadBufferSize+overhead+27)*3), getContent(p.config.OffsetsFile), "wrong offsets")
}

func TestReadComplexSeqMulti(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	file := createTempFile()

	lines := 1000
	addLines(file, lines, lines+lines)
	c.HandleEventFlowFinish(false)

	c.WaitUntilDone(false)
	assert.Equal(t, lines, c.GetEventLogLength(), "wrong log count")
	assert.Equal(t, lines, c.GetEventsTotal(), "Wrong processed events count")

	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, lines*8), getContent(p.config.OffsetsFile), "wrong offsets")
}

func TestReadComplexSeqOne(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	file := createTempFile()

	addData(file, []byte(`"`), false, false)
	lines := 100
	for i := 0; i < lines; i++ {
		addData(file, []byte{'a'}, false, false)
	}
	addData(file, []byte{}, false, false)
	addData(file, []byte(`"`), true, false)
	c.HandleEventFlowFinish(false)

	c.WaitUntilDone(false)

	assert.Equal(t, 1, c.GetEventLogLength(), "wrong log count")
	assert.Equal(t, lines+2, len(c.GetEventLogItem(0)), "Wrong log")
	assert.Equal(t, 1, c.GetEventsTotal(), "Wrong processed events count")

	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, lines+3), getContent(p.config.OffsetsFile), "wrong offsets")
}

func TestReadComplexPar(t *testing.T) {
	setup()
	defer shutdown()

	lines := 100
	files := 60
	filesNames := make([]string, 0, files)
	for i := 0; i < files; i++ {
		file := createTempFile()
		addLines(file, lines, lines+lines)

		filesNames = append(filesNames, file)
	}

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	assert.Equal(t, lines*files, c.GetEventLogLength(), "wrong log count")
	assert.Equal(t, lines*files, c.GetEventsTotal(), "Wrong processed events count")

	p.jobProvider.saveOffsets()
	assertOffsetsEqual(t, genOffsetsContentMultiple(filesNames, lines*7), getContent(p.config.OffsetsFile))
}

func TestReadHeavy(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	file := createTempFile()
	json := getContentBytes("../../../testdata/json/heavy.json")
	lines := 10
	for i := 0; i < lines; i++ {
		addData(file, json, false, true)
	}

	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	assert.Equal(t, lines, c.GetEventLogLength())
	assert.Equal(t, lines, c.GetEventsTotal())

	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, len(json)*lines), getContent(p.config.OffsetsFile), "wrong offsets")
}

func TestReadInsane(t *testing.T) {
	setup()
	defer shutdown()

	json := getContentBytes("../../../testdata/json/light.json")

	lines := 256
	files := 32
	fs := make([]*os.File, 0, files)
	fileNames := make([]string, 0, files)

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	for f := 0; f < files; f++ {
		file := createTempFile()
		f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0660)
		if err != nil {
			panic(err.Error())
		}
		fs = append(fs, f)
		fileNames = append(fileNames, file)
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(fs))
	for i := range fs {
		go func(index int) {
			for i := 0; i < lines; i++ {
				addDataFile(fs[index], json)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	p.jobProvider.saveOffsets()
	assertOffsetsEqual(t, genOffsetsContentMultiple(fileNames, len(json)*lines), getContent(p.config.OffsetsFile))
}

func TestReadInsaneLong(t *testing.T) {
	setup()
	defer shutdown()

	s := ""
	overhead := 100
	for i := 0; i < defaultReadBufferSize+overhead; i++ {
		s = s + "a"
	}
	json1 := []byte(fmt.Sprintf(`{"Data":"%s"}`+"\n"+`{"Data":"xxx"}`+"\n", s))
	json2 := []byte(fmt.Sprintf(`{"Data":"xxx"}`+"\n"))

	lines := 128
	files := 8
	fs := make([]*os.File, 0, files)
	fileNames := make([]string, 0, files)

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	for f := 0; f < files; f++ {
		file := createTempFile()
		f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0660)
		if err != nil {
			panic(err.Error())
		}
		fs = append(fs, f)
		fileNames = append(fileNames, file)
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(fs))
	for i := range fs {
		go func(index int) {
			for i := 0; i < lines; i++ {
				addDataFile(fs[index], json1)
				addDataFile(fs[index], json2)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	p.jobProvider.saveOffsets()
	assertOffsetsEqual(t, genOffsetsContentMultiple(fileNames, (len(json1)+len(json2))*lines), getContent(p.config.OffsetsFile))
}

func TestReadPar(t *testing.T) {
	setup()
	defer shutdown()

	json := getContentBytes("../../../testdata/json/light.json")

	lines := 128 * 128
	files := 32
	content := make([]byte, 0, len(json)*lines)
	for i := 0; i < lines; i++ {
		content = append(content, json...)
	}

	fileNames := make([]string, 0, files)
	for f := 0; f < files; f++ {
		file := createTempFile()
		fileNames = append(fileNames, file)
		addData(file, content, false, false)
	}

	c, p := startPipeline("async", true, nil)
	defer c.Stop()
	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	p.jobProvider.saveOffsets()
	assertOffsetsEqual(t, genOffsetsContentMultiple(fileNames, len(json)*lines), getContent(p.config.OffsetsFile))
}

func TestReadParStreams(t *testing.T) {
	setup()
	defer shutdown()

	json := getContentBytes("../../../testdata/json/streams.json")

	lines := 128
	files := 16
	content := make([]byte, 0, len(json)*lines)
	for i := 0; i < lines; i++ {
		content = append(content, json...)
	}

	fileNames := make([]string, 0, files)
	for f := 0; f < files; f++ {
		file := createTempFile()
		fileNames = append(fileNames, file)
		addData(file, content, false, false)
	}

	c, p := startPipeline("async", true, nil)
	defer c.Stop()
	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	p.jobProvider.saveOffsets()
	assertOffsetsEqual(t, genOffsetsContentMultipleStreams(fileNames, len(json)*lines, len(json)*lines-len(json)/2), getContent(p.config.OffsetsFile))
}

func TestRenameRotationHandle(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1_1"}`), true, false)
	addData(file, []byte(`{"Data":"Line2_1"}`), true, false)

	newFile := file + ".new"
	renameFile(file, newFile)
	createFile(file)

	addData(newFile, []byte(`{"Data":"Line3_1"}`), true, false)
	addData(newFile, []byte(`{"Data":"Line4_1"}`), true, false)
	addData(newFile, []byte(`{"Data":"Line5_1"}`), true, false)
	addData(newFile, []byte(`{"Data":"Line6_1"}`), true, false)

	addData(file, []byte(`{"Data":"Line1_2"}`), true, false)
	addData(file, []byte(`{"Data":"Line2_2"}`), true, false)
	addData(file, []byte(`{"Data":"Line2_2"}`), true, false)
	addData(file, []byte(`{"Data":"Line3_2"}`), true, false)
	c.HandleEventFlowFinish(false)

	c.WaitUntilDone(false)

	p.jobProvider.saveOffsets()
	offsets := fmt.Sprintf(`- file: %d %s
  not_set: 114
- file: %d %s
  not_set: 76
`, getInodeByFile(newFile), newFile, getInodeByFile(file), file)

	assert.Equal(t, 10, c.GetEventLogLength(), "wrong log count")
	assert.Equal(t, 10, c.GetEventsTotal(), "Wrong processed events count")

	assertOffsetsEqual(t, offsets, getContent(p.config.OffsetsFile))
}

// todo: this test should work but we should have some "fingerprint" of a file to do it
//func TestShutdownRotation(t *testing.T) {
//	setup()
//	defer shutdown()
//
//	c, p := startPipeline("async", true, nil)
//
//	file := createTempFile()
//	addData(file, []byte(`{"Data":"Line1_1"}`), true, false)
//	addData(file, []byte(`{"Data":"Line2_1"}`), true, false)
//	c.HandleEventFlowFinish(false)
//
//	c.WaitUntilDone(false)
//	c.Stop()
//	assert.Equal(t, 2, c.GetEventLogLength(), "wrong log count")
//	assert.Equal(t, 2, c.GetEventsTotal(), "Wrong processed events count")
//
//	newFile := rotateFile(file)
//
//	addData(newFile, []byte(`{"Data":"Line3_1"}`), true, false)
//	addData(newFile, []byte(`{"Data":"Line4_1"}`), true, false)
//	addData(newFile, []byte(`{"Data":"Line5_1"}`), true, false)
//	addData(newFile, []byte(`{"Data":"Line6_1"}`), true, false)
//
//	addData(file, []byte(`{"Data":"Line1_2"}`), true, false)
//	addData(file, []byte(`{"Data":"Line2_2"}`), true, false)
//	addData(file, []byte(`{"Data":"Line2_2"}`), true, false)
//	addData(file, []byte(`{"Data":"Line3_2"}`), true, false)
//
//	c, p = startPipeline("async", true, nil)
//	defer c.Stop()
//	c.HandleEventFlowFinish(false)
//	c.WaitUntilDone(false)
//	p.jobProvider.saveOffsets()
//
//	offsets := fmt.Sprintf(`- file: %d %s
//  not_set: 114
//- file: %d %s
//  not_set: 76
//`, getInodeByFile(newFile), newFile, getInodeByFile(file), file)
//
//	assert.Equal(t, 8, c.GetEventLogLength(), "wrong log count")
//	assert.Equal(t, 8, c.GetEventsTotal(), "Wrong processed events count")
//
//	assertOffsetsEqual(t, offsets, getContent(p.config.OffsetsFile))
//}

func TestTruncation(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	data := []byte(`{"Data":"Line1"}`)
	file := createTempFile()
	addData(file, data, true, false)
	addData(file, data, true, false)
	c.HandleEventFlowFinish(false)

	c.WaitUntilDone(false)
	assert.Equal(t, 2, c.GetEventLogLength(), "wrong log count")
	assert.Equal(t, 2, c.GetEventsTotal(), "Wrong processed events count")

	c.HandleEventFlowStart()
	truncateFile(file)
	data = []byte(`{"Data":"Line2"}`)
	addData(file, data, true, true)
	addData(file, data, true, true)
	addData(file, data, true, true)
	c.HandleEventFlowFinish(false)

	c.WaitUntilDone(false)
	assert.Equal(t, 5, c.GetEventLogLength(), "wrong log count")
	assert.Equal(t, 5, c.GetEventsTotal(), "Wrong processed events count")

	p.jobProvider.saveOffsets()
	assertOffsetsEqual(t, genOffsetsContent(file, (len(data)+1)*3), getContent(p.config.OffsetsFile))
}

func TestTruncationSeq(t *testing.T) {
	setup()
	defer shutdown()

	c, _ := startPipeline("async", true, nil)
	defer c.Stop()

	data := getContentBytes("../../../testdata/json/streams.json")
	data = append(data, data...)
	data = append(data, data...)
	data = append(data, data...)
	data = append(data, data...)
	data = append(data, data...)
	data = append(data, data...)
	data = append(data, data...)
	truncationSize := int(32 * units.Mebibyte)
	truncationCount := 4

	wg := &sync.WaitGroup{}

	for k := 0; k < 4; k++ {
		wg.Add(1)
		go func() {
			lwg := &sync.WaitGroup{}
			truncations := 0
			size := atomic.Int32{}
			name := createTempFile()
			file, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0664)
			defer func() {
				_ = file.Close()
			}()
			if err != nil {
				panic(err.Error())
			}
			lwg.Add(2)
			go func() {
				for {
					_, _ = file.Write(data)
					size.Add(int32(len(data)))
					if rand.Int()%300 == 0 {
						time.Sleep(time.Millisecond * 200)
					}
					if truncations > truncationCount {
						break
					}
				}
				lwg.Done()
			}()

			go func() {
				for {
					time.Sleep(50 * time.Millisecond)
					if size.Load() > int32(truncationSize) {
						fmt.Println("truncate")
						size.Swap(0)
						_ = file.Truncate(0)
						truncations++
						if truncations > truncationCount {
							break
						}
					}
				}
				lwg.Done()
			}()

			lwg.Wait()
			wg.Done()
		}()
	}

	wg.Wait()
	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)
}

// todo: remove sleep after file creation and fix test
func TestRenameRotationInsane(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startPipeline("async", true, nil)
	defer c.Stop()

	files := 16
	fileList := make([]string, 0, files)

	wg := sync.WaitGroup{}
	wg.Add(files)
	for i := 0; i < files; i++ {
		fileList = append(fileList, createTempFile())
		time.Sleep(time.Millisecond * 100)
		go func(file string, index int, wg *sync.WaitGroup) {
			addData(file, []byte(`{"Data":"Line1_1"}`), true, false)
			addData(file, []byte(`{"Data":"Line2_1"}`), true, false)

			newFile := file + ".new"
			renameFile(file, newFile)
			createFile(file)

			addData(file, []byte(`{"Data":"Line3_1"}`), true, false)
			addData(file, []byte(`{"Data":"Line4_1"}`), true, false)
			addData(file, []byte(`{"Data":"Line5_1"}`), true, false)
			addData(file, []byte(`{"Data":"Line6_1"}`), true, false)

			addData(newFile, []byte(`{"Data":"Line1_2"}`), true, false)
			addData(newFile, []byte(`{"Data":"Line2_2"}`), true, false)
			wg.Done()
		}(fileList[i], i, &wg)
	}

	for i := 0; i < files; i++ {
		fileList = append(fileList, fileList[i]+".new")
	}

	wg.Wait()
	p.jobProvider.maintenanceJobs()
	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	p.jobProvider.saveOffsets()

	assert.Equal(t, files*8, c.GetEventsTotal(), "Wrong processed events count")

	assertOffsetsEqual(t, genOffsetsContentMultiple(fileList, 4*19), getContent(p.config.OffsetsFile))
}

func BenchmarkRawRead(b *testing.B) {
	setup()
	defer shutdown()

	bytes := 100 * 1024 * 1024
	file := createTempFile()
	data := make([]byte, 0, bytes)
	lengthOffset := 300
	lengthRange := 300
	line := make([]byte, 0, lengthOffset+lengthRange+1)
	i := 0
	lines := 0
	for i < bytes {
		l := lengthOffset + rand.Int()%lengthRange
		if i+l > bytes {
			l -= i + l - bytes + 1
		}
		c := line[0:l]
		for j := 0; j < l; j++ {
			c[j] = 'a'
		}
		c[l-1] = '\n'
		lines++
		i += l + 1
		data = append(data, c...)
	}
	addData(file, data, false, true)

	b.ResetTimer()
	b.SetBytes(int64(bytes))
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		c, _ := startPipeline("timer", false, nil)

		c.HandleEventFlowFinish(false)
		c.WaitUntilDone(false)
		fmt.Printf("gen lines=%d, processed lines: %d\n", lines, c.GetEventsTotal())

		c.Stop()
	}
}

func BenchmarkHeavyJsonRead(b *testing.B) {
	setup()
	defer shutdown()

	file := createTempFile()
	json := getContent("../../../testdata/json/heavy.json")
	lines := 100
	content := make([]byte, 0, len(json)*lines)
	for i := 0; i < lines; i++ {
		content = append(content, json...)
	}
	addData(file, content, false, false)

	b.ResetTimer()
	b.SetBytes(int64(lines * len(json)))
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		c, _ := startPipeline("timer", false, nil)

		c.HandleEventFlowFinish(true)
		c.WaitUntilDone(true)
		fmt.Printf("gen bytes=%d, gen lines=%d, processed lines: %d\n", lines*len(json), lines, c.GetEventsTotal())

		c.Stop()
	}
}

func BenchmarkLightJsonReadSeq(b *testing.B) {
	setup()
	defer shutdown()

	file := createTempFile()
	json := getContent("../../../testdata/json/light.json")
	lines := 30000
	content := make([]byte, 0, len(json)*lines)
	for i := 0; i < lines; i++ {
		content = append(content, json...)
	}
	addData(file, content, false, false)

	b.SetBytes(int64(lines * len(json)))
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c, _ := startPipeline("timer", false, nil)

		c.HandleEventFlowFinish(true)
		c.WaitUntilDone(true)
		fmt.Printf("gen bytes=%d, gen lines=%d, processed lines: %d\n", lines*len(json), lines*2, c.GetEventsTotal())

		c.Stop()
	}
}

func BenchmarkLightJsonReadPar(b *testing.B) {
	setup()
	defer shutdown()

	lines := 128 * 128
	files := 64

	json := getContent("../../../testdata/json/light.json")

	content := make([]byte, 0, len(json)*lines)
	for i := 0; i < lines; i++ {
		content = append(content, json...)
	}

	for f := 0; f < files; f++ {
		file := createTempFile()
		addData(file, content, false, false)
	}

	bytes := int64(files * lines * len(json))
	logger.Infof("")
	logger.Infof("will read %dMb", bytes/1024/1024)
	b.SetBytes(bytes)
	b.ReportAllocs()
	b.StopTimer()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c, _ := startPipeline("timer", false, nil)
		b.StartTimer()
		c.HandleEventFlowFinish(true)
		c.WaitUntilDone(true)
		b.StopTimer()

		c.Stop()
	}
}
