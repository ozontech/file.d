package mask

import (
	"strings"
	"sync"
	"testing"

	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/test"
	"github.com/stretchr/testify/assert"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

func MustString(s *string, _ bool) string {
	return *s
}

func TestMaskFunctions(t *testing.T) {
	logger.Instance = zap.NewNop().Sugar()
	suits := []struct {
		name         string
		comment      string
		input        string
		inputRe      string
		substitution string
		expected     string
	}{
		{
			name:         "simple test",
			comment:      "all digits should be replaced",
			input:        "01.01.2021",
			inputRe:      `\d`,
			substitution: `*`,
			expected:     "**.**.****",
		},
		{
			name:         "re not matches input string",
			comment:      "no one symbol should be replaced",
			input:        "ab.cd.efgh",
			inputRe:      `\d`,
			substitution: `*`,
			expected:     "ab.cd.efgh",
		},
		{
			name:         "card number",
			comment:      "hide card number",
			input:        "1234-2345-4567-3322",
			inputRe:      `\b\d{1,4}\D?\d{1,4}\D?\d{1,4}\D?\d{1,4}\b`,
			substitution: "****-****-****-****",
			expected:     "****-****-****-****",
		},
		{
			name:         "ID",
			comment:      "hide ID",
			input:        "Иванов Иван Иванович",
			inputRe:      `^[А-Я][а-я]{1,64}(\-[А-Я][а-я]{1,64})?\s+[А-Я][а-я]{1,64}(\.)?\s+[А-Я][а-я]{1,64}`,
			substitution: "<Фамилия Имя Отчество>",
			expected:     "<Фамилия Имя Отчество>",
		},
	}

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			config := Config{[]Mask{{s.inputRe, s.substitution}}}
			sut := Plugin{config: &config}
			params := pipeline.ActionPluginParams{}
			params.Logger = logger.Instance
			sut.Start(&config, &params)
			assert.Equal(t, s.expected, MustString(sut.maskAll(s.input)), s.comment)
			assert.Equal(t, s.expected, MustString(sut.maskIfMatched(s.input)), s.comment)
			assert.Equal(t, s.expected, MustString(sut.maskByIndex(s.input)), s.comment)
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
			expected: []string{},
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
				"Images/Sun.png",
				"sun1",
				"center",
				"Click Here",
				"bold",
				"text1",
				"center",
				"sun1.opacity = (sun1.opacity / 100) * 90;"},
		},
	}

	var slice []string

	appender := func(in string) (*string, bool) {
		slice = append(slice, in)
		return &in, true
	}

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			root, err := insaneJSON.DecodeString(s.input)
			assert.NoError(t, err, "error on parsing test json")
			node := root.Node
			applyForStrings(node, appender)
			assert.Equal(t, s.expected, slice, s.comment)
			slice = slice[:0]
		})
	}
}

//nolint:funlen
func TestRemask(t *testing.T) {
	suits := []struct {
		name     string
		input    string
		expected string
		comment  string
	}{
		{
			name:     "simple substitution",
			input:    `{"field1":"-ab-axxb-"}`,
			expected: `{"field1":"-TEST-TEST-"}`,
			comment:  "only value replaced",
		},
		{
			name:     "card number substitution",
			input:    `{"field1":"4445-2222-3333-4444"}`,
			expected: `{"field1":"****-****-****-****"}`,
			comment:  "card number replaced",
		},
		{
			name:     "ID",
			input:    `{"field1":"Иванов Иван Иванович"}`,
			expected: `{"field1":"<Фамилия Имя Отчество>"}`,
			comment:  "ID replaced",
		},
		{
			name:     "ID with text",
			input:    `{"field1":"Иванов Иван Иванович встал не с той ноги"}`,
			expected: `{"field1":"<Фамилия Имя Отчество> встал не с той ноги"}`,
			comment:  "Only ID replaced",
		},
		{
			name:     "ID&text&card",
			input:    `{"field1":"Иванов Иван Иванович c картой 4445-2222-3333-4444 встал не с той ноги"}`,
			expected: `{"field1":"<Фамилия Имя Отчество> c картой ****-****-****-**** встал не с той ноги"}`,
			comment:  "only ID & card replaced",
		},
		{
			name:     "ID&text&card",
			input:    `{"field1":"Иванов Иван Иванович c картами 4445-2222-3333-4444 и 4445-2222-3333-4444"}`,
			expected: `{"field1":"<Фамилия Имя Отчество> c картами ****-****-****-**** и ****-****-****-****"}`,
			comment:  "ID replaced, and card replaced twice",
		},
	}

	config := Config{
		Masks: []Mask{
			{
				Re:           `a(x*)b`,
				Substitution: "TEST",
			},
			{
				Re:           `\b\d{1,4}\D?\d{1,4}\D?\d{1,4}\D?\d{1,4}\b`,
				Substitution: "****-****-****-****",
			},
			{
				Re:           `^[А-Я][а-я]{1,64}(\-[А-Я][а-я]{1,64})?\s+[А-Я][а-я]{1,64}(\.)?\s+[А-Я][а-я]{1,64}`,
				Substitution: "<Фамилия Имя Отчество>",
			},
		},
	}

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			sut, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(factory, &config,
					pipeline.MatchModeAnd,
					nil,
					false))
			wg := sync.WaitGroup{}

			wg.Add(1)

			outEvents := make([]*pipeline.Event, 0)
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents = append(outEvents, e)
				wg.Done()
			})

			input.In(0, "test.log", 0, []byte(s.input))

			wg.Wait()
			sut.Stop()

			assert.Equal(t, s.expected, outEvents[0].Root.EncodeToString(), s.comment)
		})
	}
}

func createBenchInput() (string, Config) {
	config := Config{
		Masks: []Mask{
			{
				Re:           `a(x*)b`,
				Substitution: "TEST",
			},
			{
				Re:           `\b\d{1,4}\D?\d{1,4}\D?\d{1,4}\D?\d{1,4}\b`,
				Substitution: "****-****-****-****",
			},
			{
				Re:           `^[А-Я][а-я]{1,64}(\-[А-Я][а-я]{1,64})?\s+[А-Я][а-я]{1,64}(\.)?\s+[А-Я][а-я]{1,64}`,
				Substitution: "<Фамилия Имя Отчество>",
			},
		},
	}
	matchable := `{"field1":"Иванов Иван Иванович c картой 4445-2222-3333-4444 встал не с той ноги"}`
	unmatchable := `{"field1":"Просто строка которая не заменяется"}`
	matchableCoeff := 0.01 // percentage of matchable input
	n := 10
	c := (int)((float64)(n) * matchableCoeff)
	builder := strings.Builder{}
	for i := 0; i < n; i++ {
		if i <= c {
			builder.WriteString(matchable)
		} else {
			builder.WriteString(unmatchable)
		}
	}
	return builder.String(), config
}

func BenchmarkMaskAll(b *testing.B) {
	logger.Instance = zap.NewNop().Sugar()
	input, config := createBenchInput()
	p := Plugin{&config, nil, nil}
	params := pipeline.ActionPluginParams{}
	params.Logger = logger.Instance
	p.Start(&config, &params)
	for i := 0; i < b.N; i++ {
		p.maskAll(input)
	}
}

func BenchmarkMaskIfMatched(b *testing.B) {
	input, config := createBenchInput()
	p := Plugin{&config, nil, logger.Instance}
	params := pipeline.ActionPluginParams{}
	params.Logger = logger.Instance
	p.Start(&config, &params)
	for i := 0; i < b.N; i++ {
		p.maskIfMatched(input)
	}
}

func BenchmarkMaskByIndex(b *testing.B) {
	input, config := createBenchInput()
	p := Plugin{&config, nil, logger.Instance}
	params := pipeline.ActionPluginParams{}
	params.Logger = logger.Instance
	p.Start(&config, &params)
	for i := 0; i < b.N; i++ {
		p.maskByIndex(input)
	}
}
