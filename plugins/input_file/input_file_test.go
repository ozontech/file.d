package input_file

import (
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"
)

var tempDir = ""

func setup() {
	t, err := ioutil.TempDir("", "input_file")
	if err != nil {
		panic(err.Error())
	}

	tempDir = t
}

func shutdown() {
	err := os.RemoveAll(tempDir)
	if err != nil {
		panic(err.Error())
	}
}

func getTestArgs(path string) *simplejson.Json {
	jsonContents := fmt.Sprintf(`{"path":"%s","is_test":true}`, path)
	json, err := simplejson.NewJson([]byte(jsonContents))
	if err != nil {
		panic(err.Error)
	}

	return json
}

func getArgs(path string) *simplejson.Json {
	jsonContents := fmt.Sprintf(`{"path":"%s"}`, path)
	json, err := simplejson.NewJson([]byte(jsonContents))
	if err != nil {
		panic(err.Error)
	}

	return json
}

func getController(isTest bool) *pipeline.Controller {
	c := pipeline.NewController(isTest)

	return c
}

func createFile() string {
	u := uuid.NewV4().String()
	file, err := os.Create(path.Join(tempDir, u))
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

	defer f.Close()

	if _, err = f.Write(data); err != nil {
		panic(err.Error())
	}
	if isLine {
		if _, err = f.Write([]byte{'\n'}); err != nil {
			panic(err.Error())
		}
	}

	if (sync) {
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

	defer f.Close()

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

func TestWatchCreateFile(t *testing.T) {
	setup()
	defer shutdown()

	c := getController(true)
	p := newInputFilePlugin(getTestArgs(tempDir), c.Parsers).(*InputFilePlugin)
	p.Start()
	defer p.Stop()

	file := createFile()
	addData(file, []byte(`{"Test":"Test"}`), true, true)

	p.jobProvider.jobsDone.Wait()

	assert.Equal(t, 1, p.createEvents)

}

func TestReadLineSimple(t *testing.T) {
	setup()
	defer shutdown()

	c := getController(true)
	p := newInputFilePlugin(getTestArgs(tempDir), c.Parsers).(*InputFilePlugin)
	p.Start()
	defer p.Stop()

	file := createFile()
	addData(file, []byte(`{"Data":"Line1"}`), true, true)
	addData(file, []byte(`{"Data":"Line2"}`), true, true)
	addData(file, []byte(`{"Data":"Line3"}`), true, true)

	p.jobProvider.jobsDone.Wait()

	assert.Equal(t, 3, len(c.SplitBuffer.EventLog))
	assert.Equal(t, 3, c.SplitBuffer.EventsProcessed())
	assert.Equal(t, `{"Data":"Line1"}`, c.SplitBuffer.EventLog[0])
	assert.Equal(t, `{"Data":"Line2"}`, c.SplitBuffer.EventLog[1])
	assert.Equal(t, `{"Data":"Line3"}`, c.SplitBuffer.EventLog[2])

}

func TestReadSeq(t *testing.T) {
	setup()
	defer shutdown()

	c := getController(true)
	p := newInputFilePlugin(getTestArgs(tempDir), c.Parsers).(*InputFilePlugin)
	p.Start()
	defer p.Stop()

	file := createFile()

	addData(file, []byte(`{"Data":"Line1`), false, true)
	addData(file, []byte(`Line2`), false, true)
	addData(file, []byte(`Line3"}`), true, true)

	p.jobProvider.jobsDone.Wait()

	assert.Equal(t, 1, len(c.SplitBuffer.EventLog))
	assert.Equal(t, 1, c.SplitBuffer.EventsProcessed())
	assert.Equal(t, `{"Data":"Line1Line2Line3"}`, c.SplitBuffer.EventLog[0])
}

func TestReadComplexSeqMulti(t *testing.T) {
	setup()
	defer shutdown()

	c := getController(true)
	p := newInputFilePlugin(getTestArgs(tempDir), c.Parsers).(*InputFilePlugin)
	p.Start()
	defer p.Stop()

	file := createFile()

	lines := 100
	addLines(file, lines, lines+lines)

	p.jobProvider.jobsDone.Wait()

	assert.Equal(t, lines, len(c.SplitBuffer.EventLog), "Event log count isn't match")
	assert.Equal(t, lines, c.SplitBuffer.EventsProcessed(), "Events processed isn't match")
}

func TestReadComplexSeqOne(t *testing.T) {
	setup()
	defer shutdown()

	c := getController(true)
	p := newInputFilePlugin(getTestArgs(tempDir), c.Parsers).(*InputFilePlugin)
	p.Start()
	defer p.Stop()

	file := createFile()

	addData(file, []byte(`"`), false, false)
	lines := 100
	for i := 0; i < lines; i++ {
		addData(file, []byte{'a'}, false, false)
	}
	addData(file, []byte{}, false, false)
	addData(file, []byte(`"`), true, false)

	p.jobProvider.jobsDone.Wait()
	time.Sleep(time.Millisecond * 100)

	assert.Equal(t, 1, len(c.SplitBuffer.EventLog), "Event log count isn't match")
	assert.Equal(t, lines+2, len(c.SplitBuffer.EventLog[0]), "Wrong log")
	assert.Equal(t, 1, c.SplitBuffer.EventsProcessed(), "Events processed isn't match")
}

func TestReadComplexPar(t *testing.T) {
	setup()
	defer shutdown()

	lines := 100
	files := 60
	for i := 0; i < files; i++ {
		file := createFile()
		addLines(file, 0, lines)
	}

	c := getController(true)
	p := newInputFilePlugin(getTestArgs(tempDir), c.Parsers).(*InputFilePlugin)
	p.Start()
	defer p.Stop()

	p.jobProvider.jobsDone.Wait()
	time.Sleep(time.Millisecond * 100)

	assert.Equal(t, lines*files, len(c.SplitBuffer.EventLog), "Event log count isn't match")
	assert.Equal(t, lines*files, c.SplitBuffer.EventsProcessed(), "Events processed isn't match")
}

func TestReadHeavy(t *testing.T) {
	setup()
	defer shutdown()

	c := getController(true)
	p := newInputFilePlugin(getTestArgs(tempDir), c.Parsers).(*InputFilePlugin)
	p.Start()
	defer p.Stop()

	file := createFile()
	json, err := ioutil.ReadFile("../../testdata/json/heavy.json")
	if err != nil {
		panic(err)
	}
	lines := 10
	for i := 0; i < lines; i++ {
		addData(file, json, false, true)
	}

	p.jobProvider.jobsDone.Wait()

	assert.Equal(t, 1, p.createEvents)
	assert.Equal(t, lines, len(c.SplitBuffer.EventLog))
	assert.Equal(t, lines, c.SplitBuffer.EventsProcessed())
}

func TestReadInsane(t *testing.T) {
	setup()
	defer shutdown()

	json, err := ioutil.ReadFile("../../testdata/json/light.json")
	if err != nil {
		panic(err)
	}

	lines := 2000
	files := 32
	for f := 0; f < files; f++ {
		file := createFile()
		for i := 0; i < lines; i++ {
			addData(file, json, false, false)
		}
	}

	c := getController(false)
	p := newInputFilePlugin(getArgs(tempDir), c.Parsers).(*InputFilePlugin)
	p.Start()
	defer p.Stop()

	p.jobProvider.jobsDone.Wait()
	fmt.Printf("gen bytes=%d, gen lines=%d, processed lines: %d, max capacity=%d\n",
		files*lines*len(json),
		files*lines*2,
		c.SplitBuffer.EventsProcessed(),
		c.SplitBuffer.MaxCapacityUsage,
	)

}

func BenchmarkRawRead(b *testing.B) {
	setup()
	defer shutdown()

	bytes := 100 * 1024 * 1024
	file := createFile()
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
		c := getController(false)
		p := newInputFilePlugin(getArgs(tempDir), c.Parsers).(*InputFilePlugin)
		p.Start()

		p.jobProvider.jobsDone.Wait()
		fmt.Printf("gen lines=%d, processed lines: %d\n", lines, c.SplitBuffer.EventsProcessed())

		p.Stop()
	}
}

func BenchmarkHeavyJsonRead(b *testing.B) {
	setup()
	defer shutdown()

	file := createFile()
	json, err := ioutil.ReadFile("../../testdata/json/heavy.json")
	if err != nil {
		panic(err)
	}
	lines := 100
	for i := 0; i < lines; i++ {
		addData(file, json, false, false)
	}

	b.ResetTimer()
	b.SetBytes(int64(lines * len(json)))
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		c := getController(false)
		p := newInputFilePlugin(getArgs(tempDir), c.Parsers).(*InputFilePlugin)
		p.Start()

		p.jobProvider.jobsDone.Wait()
		fmt.Printf("gen bytes=%d, gen lines=%d, processed lines: %d, max stream length=%d\n", lines*len(json), lines, c.SplitBuffer.EventsProcessed(), c.SplitBuffer.MaxCapacityUsage)

		p.Stop()
	}
}

func BenchmarkLightJsonReadSeq(b *testing.B) {
	setup()
	defer shutdown()

	file := createFile()
	json, err := ioutil.ReadFile("../../testdata/json/light.json")
	if err != nil {
		panic(err)
	}
	lines := 30000
	for i := 0; i < lines; i++ {
		addData(file, json, false, false)
	}

	c := getController(false)
	p := newInputFilePlugin(getArgs(tempDir), c.Parsers).(*InputFilePlugin)
	b.SetBytes(int64(lines * len(json)))
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p.Start()

		p.jobProvider.jobsDone.Wait()
		fmt.Printf("gen bytes=%d, gen lines=%d, processed lines: %d, max stream length=%d\n", lines*len(json), lines*2, c.SplitBuffer.EventsProcessed(), c.SplitBuffer.MaxCapacityUsage)

		p.Stop()
	}
}

func BenchmarkLightJsonReadPar(b *testing.B) {
	setup()
	defer shutdown()

	json, err := ioutil.ReadFile("../../testdata/json/light.json")
	if err != nil {
		panic(err)
	}

	lines := 2000
	files := 32
	for f := 0; f < files; f++ {
		file := createFile()
		for i := 0; i < lines; i++ {
			addData(file, json, false, false)
		}
	}

	c := getController(false)
	p := newInputFilePlugin(getArgs(tempDir), c.Parsers).(*InputFilePlugin)
	b.SetBytes(int64(files * lines * len(json)))
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p.Start()

		p.jobProvider.jobsDone.Wait()
		fmt.Printf("gen bytes=%d, gen lines=%d, processed lines: %d, max capacity=%d\n",
			files*lines*len(json),
			files*lines*2,
			c.SplitBuffer.EventsProcessed(),
			c.SplitBuffer.MaxCapacityUsage,
		)

		p.Stop()
	}
}
