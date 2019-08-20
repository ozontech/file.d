package input_file

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

var (
	filesDir   = ""
	offsetsDir = ""
)

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

func startController(persistenceMode string, enableEventLog bool, shouldWaitForJob bool, config *Config) (*pipeline.Controller, *InputFilePlugin) {
	controller := pipeline.NewController(enableEventLog, shouldWaitForJob)

	if config == nil {
		config = &Config{WatchingDir: filesDir, OffsetsFilename: filepath.Join(offsetsDir, "filed.offsets"), PersistenceMode: persistenceMode}
	}
	inputPlugin := newInputFilePlugin(config, controller)

	controller.SetInputPlugin(inputPlugin)
	controller.Start()

	return controller, inputPlugin.(*InputFilePlugin)
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

func addData(file string, data []byte, isLine bool, sync bool) {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
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
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
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
  default: %d
`, getInode(file), file, offset)
}

func genOffsetsContentMultiple(files []string, offset int) string {
	result := make([]byte, 0, len(files)*100)
	for _, file := range files {
		result = append(result, fmt.Sprintf(`- file: %d %s
  default: %d
`, getInode(file), file, offset)...)
	}

	return string(result)
}

func getInode(file string) uint64 {
	stat, err := os.Stat(file)
	if err != nil {
		panic(err)
	}
	sysStat := stat.Sys().(*syscall.Stat_t)
	inode := sysStat.Ino
	return inode
}

func assertOffsetsEqual(t *testing.T, offsetsContentA string, offsetsContentB string) {
	offsetsA, _ := parseOffsets(offsetsContentA)
	offsetsB, _ := parseOffsets(offsetsContentB)
	for sourceId, streams := range offsetsA {
		_, has := offsetsB[sourceId]
		assert.True(t, has, "Offsets aren't equal, sourceId %d", sourceId)
		for stream, offset := range streams {
			_, has := offsetsB[sourceId][stream]
			assert.True(t, has, "Offsets aren't equal, no stream %q", stream)
			assert.Equal(t, offset, offsetsB[sourceId][stream], "Offsets aren't equal")
		}
	}
}

func TestWatchCreateFile(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startController("async", true, true, nil)
	defer c.Stop()

	file := createTempFile()
	addData(file, []byte(`{"Test":"Test"}`), true, true)

	assert.Equal(t, 1, p.watcher.filesCreated, "Watch failed")

}

func TestReadLineSimple(t *testing.T) {
	setup()
	defer shutdown()

	c, _ := startController("async", true, true, nil)
	defer c.Stop()

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1"}`), true, true)
	addData(file, []byte(`{"Data":"Line2"}`), true, true)
	addData(file, []byte(`{"Data":"Line3"}`), true, true)

	c.WaitUntilDone()

	assert.Equal(t, 3, c.GetEventLogLength(), "Wrong log count")
	assert.Equal(t, 3, c.EventsProcessed(), "Wrong log count")
	assert.Equal(t, `{"Data":"Line1"}`, c.GetEventLogItem(0), "Wrong log")
	assert.Equal(t, `{"Data":"Line2"}`, c.GetEventLogItem(1), "Wrong log")
	assert.Equal(t, `{"Data":"Line3"}`, c.GetEventLogItem(2), "Wrong log")

}

func TestOffsetsSimple(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startController("async", true, true, nil)
	defer c.Stop()

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1"}`), true, false)
	addData(file, []byte(`{"Data":"Line2"}`), true, false)
	addData(file, []byte(`{"Data":"Line3"}`), true, false)

	c.WaitUntilDone()

	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, 51), getContent(p.config.OffsetsFilename), "Wrong offsets")
}

func TestLoadOffsets(t *testing.T) {
	setup()
	defer shutdown()

	file := createTempFile()
	offsets := genOffsetsContent(file, 100)

	addData(file, []byte(offsets), false, false)

	config := &Config{OffsetsFilename: file, PersistenceMode: "async", WatchingDir: offsetsDir}
	c, p := startController("async", true, false, config)
	defer c.Stop()

	config.OffsetsFilename += ".new"
	p.jobProvider.saveOffsets()
	assert.Equal(t, offsets, getContent(p.config.OffsetsFilename), "Wrong offsets")
}

func TestContinueReading(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startController("async", true, true, nil)

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1"}`), true, false)
	addData(file, []byte(`{"Data":"Line2"}`), true, false)
	addData(file, []byte(`{"Data":"Line3"}`), true, false)

	c.WaitUntilDone()
	c.Stop()
	p.jobProvider.saveOffsets()
	assert.Equal(t, 3, c.GetEventLogLength(), "Wrong log count")
	assert.Equal(t, genOffsetsContent(file, 51), getContent(p.config.OffsetsFilename), "Wrong offsets")

	addData(file, []byte(`{"Data":"Line4"}`), true, false)
	addData(file, []byte(`{"Data":"Line5"}`), true, false)
	addData(file, []byte(`{"Data":"Line6"}`), true, false)
	addData(file, []byte(`{"Data":"Line7"}`), true, false)

	c, p = startController("async", true, true, nil)

	c.WaitUntilDone()
	c.Stop()
	assert.Equal(t, 4, c.GetEventLogLength(), "Wrong log count")
	assert.Equal(t, `{"Data":"Line4"}`, c.GetEventLogItem(0), "Wrong log")
	assert.Equal(t, `{"Data":"Line5"}`, c.GetEventLogItem(1), "Wrong log")
	assert.Equal(t, `{"Data":"Line6"}`, c.GetEventLogItem(2), "Wrong log")
	assert.Equal(t, `{"Data":"Line7"}`, c.GetEventLogItem(3), "Wrong log")
	assert.Equal(t, genOffsetsContent(file, 119), getContent(p.config.OffsetsFilename), "Wrong offsets")
}

func TestReadSeq(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startController("async", true, true, nil)
	defer c.Stop()

	file := createTempFile()

	addData(file, []byte(`{"Data":"Line1`), false, true)
	addData(file, []byte(`Line2`), false, true)
	addData(file, []byte(`Line3"}`), true, true)

	c.WaitUntilDone()

	assert.Equal(t, 1, c.GetEventLogLength(), "Wrong log count")
	assert.Equal(t, 1, c.EventsProcessed(), "Wrong log count")
	assert.Equal(t, `{"Data":"Line1Line2Line3"}`, c.GetEventLogItem(0), "Wrong log")

	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, 27), getContent(p.config.OffsetsFilename), "Wrong offsets")
}

func TestReadComplexSeqMulti(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startController("async", true, true, nil)
	defer c.Stop()

	file := createTempFile()

	lines := 1000
	addLines(file, lines, lines+lines)

	c.WaitUntilDone()
	assert.Equal(t, lines, c.GetEventLogLength(), "Wrong log count")
	assert.Equal(t, lines, c.EventsProcessed(), "Wrong processed events count")

	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, lines*8), getContent(p.config.OffsetsFilename), "Wrong offsets")
}

func TestReadComplexSeqOne(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startController("async", true, true, nil)
	defer c.Stop()

	file := createTempFile()

	addData(file, []byte(`"`), false, false)
	lines := 100
	for i := 0; i < lines; i++ {
		addData(file, []byte{'a'}, false, false)
	}
	addData(file, []byte{}, false, false)
	addData(file, []byte(`"`), true, false)

	c.WaitUntilDone()

	assert.Equal(t, 1, c.GetEventLogLength(), "Wrong log count")
	assert.Equal(t, lines+2, len(c.GetEventLogItem(0)), "Wrong log")
	assert.Equal(t, 1, c.EventsProcessed(), "Wrong processed events count")

	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, lines+3), getContent(p.config.OffsetsFilename), "Wrong offsets")
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

	c, p := startController("async", true, true, nil)
	defer c.Stop()

	c.WaitUntilDone()

	assert.Equal(t, lines*files, c.GetEventLogLength(), "Wrong log count")
	assert.Equal(t, lines*files, c.EventsProcessed(), "Wrong processed events count")

	p.jobProvider.saveOffsets()
	assertOffsetsEqual(t, genOffsetsContentMultiple(filesNames, lines*7), getContent(p.config.OffsetsFilename))
}

func TestReadHeavy(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startController("async", true, true, nil)
	defer c.Stop()

	file := createTempFile()
	json := getContentBytes("../../testdata/json/heavy.json")
	lines := 10
	for i := 0; i < lines; i++ {
		addData(file, json, false, true)
	}

	c.WaitUntilDone()

	assert.Equal(t, lines, c.GetEventLogLength())
	assert.Equal(t, lines, c.EventsProcessed())

	p.jobProvider.saveOffsets()
	assert.Equal(t, genOffsetsContent(file, len(json)*lines), getContent(p.config.OffsetsFilename), "Wrong offsets")
}

func TestReadInsane(t *testing.T) {
	setup()
	defer shutdown()

	json := getContentBytes("../../testdata/json/light.json")

	lines := 100
	files := 32
	filesNames := make([]string, 0, files)
	for f := 0; f < files; f++ {
		file := createTempFile()
		for i := 0; i < lines; i++ {
			addData(file, json, false, false)
		}
		filesNames = append(filesNames, file)
	}

	c, p := startController("async", true, true, nil)
	defer c.Stop()

	c.WaitUntilDone()

	p.jobProvider.saveOffsets()
	assertOffsetsEqual(t, genOffsetsContentMultiple(filesNames, len(json)*lines), getContent(p.config.OffsetsFilename))
}

func TestRenameRotationHandle(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startController("async", true, true, nil)
	defer c.Stop()

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1_1"}`), true, false)
	addData(file, []byte(`{"Data":"Line2_1"}`), true, false)

	//c.WaitUntilDone()

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

	c.WaitUntilDone()
	p.jobProvider.saveOffsets()
	offsets := fmt.Sprintf(`- file: %d %s
  default: 114
- file: %d %s
  default: 76
`, getInode(newFile), newFile, getInode(file), file)

	assert.Equal(t, 10, c.GetEventLogLength(), "Wrong log count")
	assert.Equal(t, 10, c.EventsProcessed(), "Wrong processed events count")

	assertOffsetsEqual(t, offsets, getContent(p.config.OffsetsFilename))
}

func TestRenameRotationInsane(t *testing.T) {
	setup()
	defer shutdown()

	c, p := startController("async", true, true, nil)
	defer c.Stop()

	files := 64
	fileList := make([]string, 0, files)

	for i := 0; i < files; i++ {
		fileList = append(fileList, createTempFile())
		go func(file string, index int) {
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
		}(fileList[i], i)
	}

	for i := 0; i < files; i++ {
		fileList = append(fileList, fileList[i]+".new")
	}

	c.WaitUntilDone()

	p.jobProvider.saveOffsets()

	assert.Equal(t, files*8, c.GetEventLogLength(), "Wrong log count")
	assert.Equal(t, files*8, c.EventsProcessed(), "Wrong processed events count")

	fmt.Println(getContent(p.config.OffsetsFilename))
	assertOffsetsEqual(t, genOffsetsContentMultiple(fileList, 4*19), getContent(p.config.OffsetsFilename))
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
		c, _ := startController("timer", false, true, nil)

		c.WaitUntilDone()
		fmt.Printf("gen lines=%d, processed lines: %d\n", lines, c.EventsProcessed())

		c.Stop()
	}
}

func BenchmarkHeavyJsonRead(b *testing.B) {
	setup()
	defer shutdown()

	file := createTempFile()
	json := getContent("../../testdata/json/heavy.json")
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
		c, _ := startController("timer", false, true, nil)

		c.WaitUntilDone()
		fmt.Printf("gen bytes=%d, gen lines=%d, processed lines: %d\n", lines*len(json), lines, c.EventsProcessed())

		c.Stop()
	}
}

func BenchmarkLightJsonReadSeq(b *testing.B) {
	setup()
	defer shutdown()

	file := createTempFile()
	json := getContent("../../testdata/json/light.json")
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
		c, _ := startController("timer", false, true, nil)

		c.WaitUntilDone()
		fmt.Printf("gen bytes=%d, gen lines=%d, processed lines: %d\n", lines*len(json), lines*2, c.EventsProcessed())

		c.Stop()
	}
}

func BenchmarkLightJsonReadPar(b *testing.B) {
	setup()
	defer shutdown()

	lines := 2000
	files := 64

	json := getContent("../../testdata/json/light.json")

	content := make([]byte, 0, len(json)*lines)
	for i := 0; i < lines; i++ {
		content = append(content, json...)
	}

	for f := 0; f < files; f++ {
		file := createTempFile()
		addData(file, content, false, false)
	}

	b.SetBytes(int64(files * lines * len(json)))
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c, _ := startController("timer", false, true, nil)

		c.WaitUntilDone()
		fmt.Printf("gen bytes=%d, gen lines=%d, processed lines: %d\n",
			files*lines*len(json),
			files*lines*2,
			c.EventsProcessed(),
		)

		c.Stop()
	}
}
