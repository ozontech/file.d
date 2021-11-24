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
		input    []byte
		masks    Mask
		expected []byte
		comment  string
	}{
		{
			name:     "simple test",
			input:    []byte("12.34.5678"),
			masks:    Mask{Re: `\d`, Groups: []int{0}},
			expected: []byte("**.**.****"),
			comment:  "all digits should be masked",
		},
		{
			name:     "re not matches input string",
			input:    []byte("ab.cd.efgh"),
			masks:    Mask{Re: `\d`, Groups: []int{0}},
			expected: []byte("ab.cd.efgh"),
			comment:  "no one symbol should be masked",
		},
		{
			name:     "simple substitution",
			input:    []byte(`{"field1":"-ab-axxb-"}`),
			masks:    Mask{Re: `a(x*)b`, Groups: []int{1}},
			expected: []byte(`{"field1":"-ab-a**b-"}`),
			comment:  "value masked only in first group",
		},
		{
			name:     "simple substitution",
			input:    []byte(`{"field1":"-ab-axxb-"}`),
			masks:    Mask{Re: `a(x*)b`, Groups: []int{0}},
			expected: []byte(`{"field1":"-**-****-"}`),
			comment:  "all value masked",
		},
		{
			name:     "card number",
			input:    []byte("1234-2345-4567-3322"),
			masks:    Mask{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3, 4}},
			expected: []byte("****-****-****-****"),
			comment:  "card number masked",
		},
		{
			name:     "groups of card number regex",
			input:    []byte("1234-2345-4567-3322"),
			masks:    Mask{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3}},
			expected: []byte("****-****-****-3322"),
			comment:  "first, second, third sections of card number masked",
		},
		{
			name:     "ID",
			input:    []byte("abbc Иванов Иван Иванович dss"),
			masks:    Mask{Re: kDefaultIDRegExp, Groups: []int{0}},
			expected: []byte("abbc ******************** dss"),
			comment:  "ID masked ",
		},
		{
			name:     "2 ID with text",
			input:    []byte("Иванов Иван Иванович и Петров Петр Петрович встали не с той ноги"),
			expected: []byte("******************** и ******************** встали не с той ноги"),
			masks:    Mask{Re: kDefaultIDRegExp, Groups: []int{0}},
			comment:  "2 ID masked",
		},
		{
			name:     "not exists groups numbers",
			input:    []byte("Иванов Иван Иванович встал не с той ноги"),
			expected: []byte("Иванов Иван Иванович встал не с той ноги"),
			masks:    Mask{Re: kDefaultIDRegExp, Groups: []int{33}},
			comment:  "Nothing masked",
		},
		{
			name:     "not exists groups numbers",
			input:    []byte("12.23.3456"),
			expected: []byte("12.23.3456"),
			masks:    Mask{Re: `\d`, Groups: []int{1}},
			comment:  "Nothing masked",
		},
		{
			name:     "exists groups numbers",
			input:    []byte("1234-2345-4567-3322"),
			expected: []byte("1234-****-4567-3322"),
			masks:    Mask{Re: kDefaultCardRegExp, Groups: []int{2}},
			comment:  "Only second part of card number masked",
		},
		{
			name:     "negative number of group",
			input:    []byte("Иванов Иван Иванович встал не с той ноги"),
			expected: []byte("Иванов Иван Иванович встал не с той ноги"),
			masks:    Mask{Re: kDefaultIDRegExp, Groups: []int{-5}},
			comment:  "Nothing masked",
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
func TestGetValueNodeList(t *testing.T) {
	suits := []struct {
		name     string
		input    string
		expected []string
		comment  string
	}{
		{
			name:     "simple test",
			input:    `{"name1":"value1"}`,
			expected: []string{"value1"},
			comment:  "one string",
		},
		{
			name:     "json with only one integer value",
			input:    `{"name1":1}`,
			expected: []string{"1"},
			comment:  "integer also included into result",
		},
		{
			name: "big json with ints and nulls",
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
			comment: "all values should be collected",
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
			comment:  "card number masked",
		},
		{
			name:     "ID",
			input:    []string{`{"field1":"Иванов Иван Иванович"}`},
			expected: []string{`{"field1":"********************"}`},
			comment:  "ID masked",
		},
		{
			name:     "ID with text",
			input:    []string{`{"field1":"Иванов Иван Иванович встал не с той ноги"}`},
			expected: []string{`{"field1":"******************** встал не с той ноги"}`},
			comment:  "only ID masked",
		},
		{
			name:     "ID&text&card",
			input:    []string{`{"field1":"Иванов Иван Иванович c картой 4445-2222-3333-4444 встал не с той ноги"}`},
			expected: []string{`{"field1":"******************** c картой ****-****-****-**** встал не с той ноги"}`},
			comment:  "only ID & card number masked",
		},
		{
			name:     "ID&text&card",
			input:    []string{`{"field1":"Иванов Иван Иванович c картами 4445-2222-3333-4444 и 4445-2222-3333-4444"}`},
			expected: []string{`{"field1":"******************** c картами ****-****-****-**** и ****-****-****-****"}`},
			comment:  "ID masked, two card numbers also masked",
		},
		{
			name:     "ID&text[cyr/en]&card",
			input:    []string{`{"field1":"yesterday Иванов Иван Иванович paid by card номер 4445-2222-3333-4444"}`},
			expected: []string{`{"field1":"yesterday ******************** paid by card номер ****-****-****-****"}`},
			comment:  "ID masked, and card number masked",
		},
		{
			name: "ID&text&card",
			input: []string{
				`{"field1":"Иванов Иван Иванович with card 4445-2222-3333-4444 gets up with the wrong side"}`,
				`{"field2":"Simple event"}`,
				`{"field3":"Просто событие"}`,
				`{"field4":"Петров Петр Петрович"}`,
			},
			expected: []string{
				`{"field1":"******************** with card ****-****-****-**** gets up with the wrong side"}`,
				`{"field2":"Simple event"}`,
				`{"field3":"Просто событие"}`,
				`{"field4":"********************"}`,
			},
			comment: "only ID & card number masked",
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

func BenchmarkMaskValue(b *testing.B) {
	input := createBenchInputString()
	re := regexp.MustCompile(kDefaultCardRegExp)
	grp := []int{0, 1, 2, 3}
	buf := make([]byte, 0, 2048)
	for i := 0; i < b.N; i++ {
		buf = maskValue(input, buf, re, grp, zap.NewNop().Sugar())
	}
}
