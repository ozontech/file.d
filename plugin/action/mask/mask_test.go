package mask

import (
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/test"
	"github.com/stretchr/testify/assert"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

const (
	kDefaultIDRegExp   = `[А-Я][а-я]{1,64}(\-[А-Я][а-я]{1,64})?\s+[А-Я][а-я]{1,64}(\.)?\s+[А-Я][а-я]{1,64}`
	kDefaultCardRegExp = `\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b`
)

//nolint:funlen
func TestMaskFunctions(t *testing.T) {
	suits := []struct {
		name     string
		comment  string
		input    []byte
		masks    Mask
		expected []byte
	}{
		{
			name:     "simple test",
			comment:  "all digits should be replaced",
			input:    []byte("12.34.5678"),
			masks:    Mask{Re: `\d`, Groups: []int{0}},
			expected: []byte("**.**.****"),
		},
		{
			name:     "re not matches input string",
			comment:  "no one symbol should be replaced",
			input:    []byte("ab.cd.efgh"),
			masks:    Mask{Re: `\d`, Groups: []int{0}},
			expected: []byte(""),
		},
		{
			name:     "simple substitution",
			comment:  "value replaced only in first group",
			input:    []byte(`{"field1":"-ab-axxb-"}`),
			masks:    Mask{Re: `a(x*)b`, Groups: []int{1}},
			expected: []byte(`{"field1":"-ab-a**b-"}`),
		},
		{
			name:     "simple substitution",
			comment:  "all value replaced",
			input:    []byte(`{"field1":"-ab-axxb-"}`),
			masks:    Mask{Re: `a(x*)b`, Groups: []int{0}},
			expected: []byte(`{"field1":"-**-****-"}`),
		},
		{
			name:     "card number",
			comment:  "hide card number",
			input:    []byte("1234-2345-4567-3322"),
			masks:    Mask{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3, 4}},
			expected: []byte("****-****-****-****"),
		},
		{
			name:     "card number",
			comment:  "hide card number",
			input:    []byte("1234-2345-4567-3322"),
			masks:    Mask{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3}},
			expected: []byte("****-****-****-3322"),
		},
		{
			name:     "ID",
			comment:  "hide ID",
			input:    []byte("abbc Иванов Иван Иванович dss"),
			masks:    Mask{Re: kDefaultIDRegExp, Groups: []int{0}},
			expected: []byte("abbc ******************** dss"),
		},
		{
			name:     "2 ID with text",
			input:    []byte("Иванов Иван Иванович и Петров Петр Петрович встали не с той ноги"),
			expected: []byte("******************** и ******************** встали не с той ноги"),
			masks:    Mask{Re: kDefaultIDRegExp, Groups: []int{0}},
			comment:  "Only ID replaced",
		},
		{
			name:     "Test not exists groups numbers",
			input:    []byte("Иванов Иван Иванович встал не с той ноги"),
			expected: []byte("Иванов Иван Иванович встал не с той ноги"),
			masks:    Mask{Re: kDefaultIDRegExp, Groups: []int{33}},
			comment:  "Only ID replaced",
		},
		{
			name:     "Test not exists groups numbers",
			input:    []byte("12.23.3456"),
			expected: []byte("12.23.3456"),
			masks:    Mask{Re: `\d`, Groups: []int{1}},
			comment:  "Nothing masked",
		},
		{
			name:     "Test groups numbers",
			input:    []byte("1234-2345-4567-3322"),
			expected: []byte("1234-****-4567-3322"),
			masks:    Mask{Re: kDefaultCardRegExp, Groups: []int{2}},
			comment:  "Only ID replaced",
		},
		{
			name:     "Test groups with negative numbers",
			input:    []byte("Иванов Иван Иванович встал не с той ноги"),
			expected: []byte("Иванов Иван Иванович встал не с той ноги"),
			masks:    Mask{Re: kDefaultIDRegExp, Groups: []int{-5}},
			comment:  "Only ID replaced",
		},
	}

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			buf := make([]byte, 0, 2048)
			buf = maskValue(s.input, buf, regexp.MustCompile(s.masks.Re), s.masks.Groups, zap.NewNop().Sugar())
			assert.Equal(t, string(s.expected), string(buf), s.comment)
		})
	}
}

//nolint:funlen
func TestApllyForStrings(t *testing.T) {
	suits := []struct {
		name     string
		comment  string
		input    string
		expected []string
	}{
		{
			name:     "simple test",
			comment:  "only one string for apply",
			input:    `{"name1":"value1"}`,
			expected: []string{"value1"},
		},
		{
			name:     "json without strings",
			comment:  "empty slice as result",
			input:    `{"name1":1}`,
			expected: []string{"1"},
		},
		{
			name:    "big json with ints and nulls",
			comment: "only strings should be collected",
			input: `{"widget": {
		    "debug": "on",
		    "window": {
		        "title": "Sample Konfabulator Widget",
		        "name": "main_window",
		        "width": 500,
		        "height": 500
		    },
		    "image": {
		        "src": "Images/Sun.png",
		        "name": "sun1",
		        "hOffset": 250,
		        "vOffset": 250,
		        "alignment": "center"
		    },
		    "text": {
		        "data": "Click Here",
		        "size": 36,
				"param": null,
		        "style": "bold",
		        "name": "text1",
		        "hOffset": 250,
		        "vOffset": 100,
		        "alignment": "center",
		        "onMouseUp": "sun1.opacity = (sun1.opacity / 100) * 90;"
		    }
		}} `,
			expected: []string{"on",
				"Sample Konfabulator Widget",
				"main_window",
				"500",
				"500",
				"Images/Sun.png",
				"sun1",
				"250",
				"250",
				"center",
				"Click Here",
				"36",
				"null",
				"bold",
				"text1",
				"250",
				"100",
				"center",
				"sun1.opacity = (sun1.opacity / 100) * 90;"},
		},
	}

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			root, err := insaneJSON.DecodeString(s.input)
			assert.NoError(t, err, "error on parsing test json")
			nodes := make([]*insaneJSON.Node, 0)
			nodes = getValueNodeList(root.Node, nodes)
			assert.Equal(t, len(nodes), len(s.expected), s.comment)
			for i := range nodes {
				assert.Equal(t, s.expected[i], nodes[i].AsString(), s.comment)
			}
		})
	}
}

//nolint:funlen
func TestPlugin(t *testing.T) {
	suits := []struct {
		name     string
		input    []string
		expected []string
		comment  string
	}{
		{
			name:     "card number substitution",
			input:    []string{`{"field1":"4445-2222-3333-4444"}`},
			expected: []string{`{"field1":"****-****-****-****"}`},
			comment:  "card number replaced",
		},
		{
			name:     "ID",
			input:    []string{`{"field1":"Иванов Иван Иванович"}`},
			expected: []string{`{"field1":"********************"}`},
			comment:  "ID replaced",
		},
		{
			name:     "ID with text",
			input:    []string{`{"field1":"Иванов Иван Иванович встал не с той ноги"}`},
			expected: []string{`{"field1":"******************** встал не с той ноги"}`},
			comment:  "Only ID replaced",
		},
		{
			name:     "ID&text&card",
			input:    []string{`{"field1":"Иванов Иван Иванович c картой 4445-2222-3333-4444 встал не с той ноги"}`},
			expected: []string{`{"field1":"******************** c картой ****-****-****-**** встал не с той ноги"}`},
			comment:  "only ID & card replaced",
		},
		{
			name:     "ID&text&card",
			input:    []string{`{"field1":"Иванов Иван Иванович c картами 4445-2222-3333-4444 и 4445-2222-3333-4444"}`},
			expected: []string{`{"field1":"******************** c картами ****-****-****-**** и ****-****-****-****"}`},
			comment:  "ID replaced, and card replaced twice",
		},
		{
			name:     "ID&text[cyr/en]&card",
			input:    []string{`{"field1":"yesterday Иванов Иван Иванович paid by card номер 4445-2222-3333-4444"}`},
			expected: []string{`{"field1":"yesterday ******************** paid by card номер ****-****-****-****"}`},
			comment:  "ID replaced, and card replaced",
		},
		{
			name: "ID&text&card",
			input: []string{
				`{"field1":"Иванов Иван Иванович c картой 4445-2222-3333-4444 встал не с той ноги"}`,
				`{"field1":"Просто событие"}`,
				`{"field1":"Петров Петр Петрович"}`,
			},
			expected: []string{
				`{"field1":"******************** c картой ****-****-****-**** встал не с той ноги"}`,
				`{"field1":"Просто событие"}`,
				`{"field1":"********************"}`,
			},
			comment: "only ID & card replaced",
		},
	}

	config := createConfig()

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			sut, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(factory, &config,
					pipeline.MatchModeAnd,
					nil,
					false))
			wg := sync.WaitGroup{}
			wg.Add(len(s.input))

			outEvents := make([]*pipeline.Event, 0)
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents = append(outEvents, e)
				wg.Done()
			})

			for _, in := range s.input {
				input.In(0, "test.log", 0, []byte(in))
			}

			wg.Wait()
			sut.Stop()

			for i := range s.expected {
				assert.Equal(t, s.expected[i], outEvents[i].Root.EncodeToString(), s.comment)
			}
		})
	}
}

func createConfig() Config {
	config := Config{
		Masks: []Mask{
			{
				Re:     `a(x*)b`,
				Groups: []int{0},
			},
			{
				Re:     kDefaultCardRegExp,
				Groups: []int{1, 2, 3, 4},
			},
			{
				Re:     kDefaultIDRegExp,
				Groups: []int{0},
			},
		},
	}
	return config
}

func createBenchInputString() []byte {
	matchable := `{"field1":"Иванов Иван Иванович c картой 4445-2222-3333-4444 встал не с той ноги"}`
	unmatchable := `{"field1":"Просто строка которая не заменяется"}`
	matchableCoeff := 0.1 // percentage of matchable input
	totalCount := 50
	matchableCount := (int)((float64)(totalCount) * matchableCoeff)
	builder := strings.Builder{}
	for i := 0; i < totalCount; i++ {
		if i <= matchableCount {
			builder.WriteString(matchable)
		} else {
			builder.WriteString(unmatchable)
		}
	}
	return []byte(builder.String())
}

func BenchmarkMask(b *testing.B) {
	input := createBenchInputString()
	re := regexp.MustCompile(kDefaultCardRegExp)
	grp := []int{0, 1, 2, 3}
	buf := make([]byte, 0, 2048)
	for i := 0; i < b.N; i++ {
		buf = maskValue(input, buf, re, grp, zap.NewNop().Sugar())
	}
}
