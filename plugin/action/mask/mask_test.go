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

const (
	kDefaultIDRegExp     = `[А-Я][а-я]{1,64}(\-[А-Я][а-я]{1,64})?\s+[А-Я][а-я]{1,64}(\.)?\s+[А-Я][а-я]{1,64}`
	kDefaultCardRegExp   = `\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b`
	kDefaultSubstitution = byte('*')
)

func MustString(p *Plugin) string {
	return string(*p.buff)
}

//nolint:funlen
func TestMaskFunctions(t *testing.T) {
	logger.Instance = zap.NewNop().Sugar()
	suits := []struct {
		name     string
		comment  string
		input    []byte
		masks    []Mask
		expected string
	}{
		{
			name:     "simple test",
			comment:  "all digits should be replaced",
			input:    []byte("12.34.5678"),
			masks:    []Mask{{`\d`, kDefaultSubstitution, []int{}}},
			expected: "**.**.****",
		},
		{
			name:     "simple test with empty groups",
			comment:  "all digits should be replaced. empty groups equal group with group '0'",
			input:    []byte("12.34.5678"),
			masks:    []Mask{{`\d`, kDefaultSubstitution, []int{0}}},
			expected: "**.**.****",
		},
		{
			name:     "re not matches input string",
			comment:  "no one symbol should be replaced",
			input:    []byte("ab.cd.efgh"),
			masks:    []Mask{{`\d`, kDefaultSubstitution, []int{0}}},
			expected: "ab.cd.efgh",
		},
		{
			name:     "simple substitution",
			comment:  "value replaced only in first group",
			input:    []byte(`{"field1":"-ab-axxb-"}`),
			masks:    []Mask{{`a(x*)b`, kDefaultSubstitution, []int{1}}},
			expected: `{"field1":"-ab-a**b-"}`,
		},
		{
			name:     "simple substitution",
			comment:  "all value replaced",
			input:    []byte(`{"field1":"-ab-axxb-"}`),
			masks:    []Mask{{`a(x*)b`, kDefaultSubstitution, []int{0}}},
			expected: `{"field1":"-**-****-"}`,
		},
		{
			name:    "many substitutions",
			comment: "value replaced in first and in all groups",
			input:   []byte(`{"field":"-ab-axxb-17-ab-axxb-ab"}`),
			masks: []Mask{{`a(x*)b`, kDefaultSubstitution, []int{0}},
				{`\d`, kDefaultSubstitution, []int{0}}},
			expected: `{"field":"-**-****-**-**-****-**"}`,
		},
		{
			name:     "many different length substitutions ",
			comment:  "value replaced",
			input:    []byte(`{"field":"-axxxxxxxxb-axb-17-ab-axxxxxxxb-ab"}`),
			masks:    []Mask{{`a(x*)b`, kDefaultSubstitution, []int{1}}, {`\d`, kDefaultSubstitution, []int{0}}},
			expected: `{"field":"-a********b-a*b-**-ab-a*******b-ab"}`,
		},
		{
			name:     "card number",
			comment:  "hide card number",
			input:    []byte("1234-2345-4567-3322"),
			masks:    []Mask{{kDefaultCardRegExp, kDefaultSubstitution, []int{1, 2, 3, 4}}},
			expected: "****-****-****-****",
		},
		{
			name:     "card number",
			comment:  "hide card number",
			input:    []byte("1234-2345-4567-3322"),
			masks:    []Mask{{kDefaultCardRegExp, kDefaultSubstitution, []int{1, 2, 3}}},
			expected: "****-****-****-3322",
		},
		{
			name:    "ID",
			comment: "hide ID",
			input:   []byte("abbc Иванов Иван Иванович dss"),
			masks: []Mask{
				{
					kDefaultIDRegExp,
					kDefaultSubstitution,
					[]int{0},
				},
			},
			expected: "abbc ******************** dss",
		},
		{
			name:    "ID",
			comment: "hide ID",
			input:   []byte("Иванов Иван Иванович"),
			masks: []Mask{
				{
					kDefaultIDRegExp,
					kDefaultSubstitution,
					[]int{0},
				},
			},
			expected: "********************",
		},
		{
			name:     "ID with text",
			input:    []byte("Иванов Иван Иванович встал не с той ноги"),
			expected: "******************** встал не с той ноги",
			masks: []Mask{
				{
					kDefaultIDRegExp,
					kDefaultSubstitution,
					[]int{0},
				},
			},
			comment: "Only ID replaced",
		},
		{
			name:     "ID with text",
			input:    []byte("сегодня Иванов Иван Иванович встал не с той ноги"),
			expected: "сегодня ******************** встал не с той ноги",
			masks: []Mask{
				{
					kDefaultIDRegExp,
					kDefaultSubstitution,
					[]int{0},
				},
			},
			comment: "Only ID replaced",
		},
	}

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			config := Config{s.masks}
			sut := Plugin{config: &config}
			params := createActionPluginParams()
			sut.Start(&config, &params)
			sut.mask(&s.input)
			assert.Equal(t, s.expected, MustString(&sut), s.comment)
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
			nodes := getValueNodeList(root.Node)
			for i := range nodes {
				assert.Equal(t, s.expected[i], nodes[i].AsString(), s.comment)
			}
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
			name:     "card number substitution",
			input:    `{"field1":"4445-2222-3333-4444"}`,
			expected: `{"field1":"****-****-****-****"}`,
			comment:  "card number replaced",
		},
		{
			name:     "ID",
			input:    `{"field1":"Иванов Иван Иванович"}`,
			expected: `{"field1":"********************"}`,
			comment:  "ID replaced",
		},
		{
			name:     "ID with text",
			input:    `{"field1":"Иванов Иван Иванович встал не с той ноги"}`,
			expected: `{"field1":"******************** встал не с той ноги"}`,
			comment:  "Only ID replaced",
		},
		{
			name:     "ID&text&card",
			input:    `{"field1":"Иванов Иван Иванович c картой 4445-2222-3333-4444 встал не с той ноги"}`,
			expected: `{"field1":"******************** c картой ****-****-****-**** встал не с той ноги"}`,
			comment:  "only ID & card replaced",
		},
		{
			name:     "ID&text&card",
			input:    `{"field1":"Иванов Иван Иванович c картами 4445-2222-3333-4444 и 4445-2222-3333-4444"}`,
			expected: `{"field1":"******************** c картами ****-****-****-**** и ****-****-****-****"}`,
			comment:  "ID replaced, and card replaced twice",
		},
		{
			name:     "ID&text[cyr/en]&card",
			input:    `{"field1":"yesterday Иванов Иван Иванович pay by card номер 4445-2222-3333-4444"}`,
			expected: `{"field1":"yesterday ******************** pay by card номер ****-****-****-****"}`,
			comment:  "ID replaced, and card replaced",
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

func createConfig() Config {
	config := Config{
		Masks: []Mask{
			{
				Re:           `a(x*)b`,
				Substitution: kDefaultSubstitution,
				Groups:       []int{0},
			},
			{
				Re:           kDefaultCardRegExp,
				Substitution: kDefaultSubstitution,
				Groups:       []int{1, 2, 3, 4},
			},
			{
				Re:           kDefaultIDRegExp,
				Substitution: kDefaultSubstitution,
				Groups:       []int{0},
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

func createPipelineSettings() *pipeline.Settings {
	var s pipeline.Settings
	s.AvgLogSize = 2048
	return &s
}

func createPluginDefaultParams() *pipeline.PluginDefaultParams {
	var p pipeline.PluginDefaultParams
	p.PipelineSettings = createPipelineSettings()
	return &p
}

func createActionPluginParams() pipeline.ActionPluginParams {
	logger.Instance = zap.NewNop().Sugar()
	params := pipeline.ActionPluginParams{}
	params.PluginDefaultParams = createPluginDefaultParams()
	params.Logger = logger.Instance
	return params
}

func createPlugin(config *Config) Plugin {
	p := Plugin{config, nil, nil, &[]byte{}}
	return p
}
func BenchmarkMask(b *testing.B) {
	config := createConfig()
	params := createActionPluginParams()
	input := createBenchInputString()
	p := createPlugin(&config)
	p.Start(&config, &params)
	for i := 0; i < b.N; i++ {
		p.mask(&input)
	}
}
