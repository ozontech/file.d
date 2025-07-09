package mask

import (
	"encoding/json"
	"regexp"
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	kDefaultIDRegExp                         = `[А-Я][а-я]{1,64}(\-[А-Я][а-я]{1,64})?\s+[А-Я][а-я]{1,64}(\.)?\s+[А-Я][а-я]{1,64}`
	kDefaultCardRegExp                       = `\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b`
	kCardWithStarOrSpaceOrNoDelimitersRegExp = `\b(\d{4})\s?\-?(\d{4})\s?\-?(\d{4})\s?\-?(\d{4})\b`
	kEMailRegExp                             = `([a-z0-9]+@[a-z0-9]+\.[a-z]+)`
)

//nolint:funlen
func TestMaskFunctions(t *testing.T) {
	suits := []struct {
		name         string
		input        []byte
		masks        Mask
		expected     []byte
		comment      string
		mustBeMasked bool
	}{
		{
			name:         "simple test",
			input:        []byte("12.34.5678"),
			masks:        Mask{Re: `\d`, Groups: []int{0}},
			expected:     []byte("**.**.****"),
			comment:      "all digits should be masked",
			mustBeMasked: true,
		},
		{
			name:         "re not matches input string",
			input:        []byte("ab.cd.efgh"),
			masks:        Mask{Re: `\d`, Groups: []int{0}},
			expected:     []byte(""),
			comment:      "no one symbol should be masked",
			mustBeMasked: false,
		},
		{
			name:         "simple substitution",
			input:        []byte(`{"field1":"-ab-axxb-"}`),
			masks:        Mask{Re: `a(x*)b`, Groups: []int{1}},
			expected:     []byte(`{"field1":"-ab-a**b-"}`),
			comment:      "value masked only in first group",
			mustBeMasked: true,
		},
		{
			name:         "simple substitution",
			input:        []byte(`{"field1":"-ab-axxb-"}`),
			masks:        Mask{Re: `a(x*)b`, Groups: []int{0}},
			expected:     []byte(`{"field1":"-**-****-"}`),
			comment:      "all value masked",
			mustBeMasked: true,
		},
		{
			name:         "card number",
			input:        []byte("5408-7430-0756-2004"),
			masks:        Mask{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3, 4}},
			expected:     []byte("****-****-****-****"),
			comment:      "card number masked",
			mustBeMasked: true,
		},
		{
			name:         "groups of card number regex",
			input:        []byte("5568-2587-2420-0263"),
			masks:        Mask{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3}},
			expected:     []byte("****-****-****-0263"),
			comment:      "first, second, third sections of card number masked",
			mustBeMasked: true,
		},
		{
			name:         "ID",
			input:        []byte("user details: Иванов Иван Иванович"),
			masks:        Mask{Re: kDefaultIDRegExp, Groups: []int{0}},
			expected:     []byte("user details: ********************"),
			comment:      "ID masked ",
			mustBeMasked: true,
		},
		{
			name:         "ID-max_count",
			input:        []byte("user details: Иванов Иван Иванович"),
			masks:        Mask{Re: kDefaultIDRegExp, Groups: []int{0}, MaxCount: 10},
			expected:     []byte("user details: **********"),
			comment:      "ID masked with max_count",
			mustBeMasked: true,
		},
		{
			name:         "ID-replace_word",
			input:        []byte("user details: Иванов Иван Иванович"),
			masks:        Mask{Re: kDefaultIDRegExp, Groups: []int{0}, mode: modeReplace, ReplaceWord: "***MASKED***"},
			expected:     []byte("user details: ***MASKED***"),
			comment:      "ID masked with replace word",
			mustBeMasked: true,
		},
		{
			name:         "2 card numbers and text",
			input:        []byte("issued card number 3528-3889-3793-9946 and card number 4035-3005-3980-4083"),
			expected:     []byte("issued card number ****-****-****-**** and card number ****-****-****-****"),
			masks:        Mask{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3, 4}},
			comment:      "2 ID masked",
			mustBeMasked: true,
		},
		{
			name:         "card number with delimiter -",
			input:        []byte("card number 3528-3889-3793-9946"),
			expected:     []byte("card number ****-****-****-9946"),
			comment:      "card must be partly musked",
			masks:        Mask{Re: kCardWithStarOrSpaceOrNoDelimitersRegExp, Groups: []int{1, 2, 3}},
			mustBeMasked: true,
		},
		{
			name:         "card number with delimiter ' '",
			input:        []byte("card number 3528 3889 3793 9946"),
			expected:     []byte("card number **** **** **** 9946"),
			comment:      "card must be partly musked",
			masks:        Mask{Re: kCardWithStarOrSpaceOrNoDelimitersRegExp, Groups: []int{1, 2, 3}},
			mustBeMasked: true,
		},
		{
			name:         "card number with no delimiter",
			input:        []byte("card number 3528388937939946"),
			expected:     []byte("card number ************9946"),
			comment:      "card must be partly musked",
			masks:        Mask{Re: kCardWithStarOrSpaceOrNoDelimitersRegExp, Groups: []int{1, 2, 3}},
			mustBeMasked: true,
		},
		{
			name:         "Individual entrepreneur",
			input:        []byte("Individual entrepreneur Ivanov Ivan Ivanovich"),
			expected:     []byte("Individual entrepreneur Ivanov Ivan Ivanovich"),
			comment:      "do not replace matched value",
			masks:        Mask{Re: "Individual entrepreneur"},
			mustBeMasked: true,
		},
		{
			name:         "email",
			input:        []byte("email login@domain.ru"),
			expected:     []byte("email SECMASKED"),
			comment:      "do not replace email",
			masks:        Mask{Re: kEMailRegExp, mode: modeReplace, ReplaceWord: "SECMASKED", Groups: []int{0}, MaxCount: 10},
			mustBeMasked: true,
		},
		{
			name:         "cut email",
			input:        []byte("email login@domain.ru"),
			expected:     []byte("email "),
			comment:      "do not cut email",
			masks:        Mask{Re: kEMailRegExp, mode: modeCut, CutValues: true, Groups: []int{0}, MaxCount: 10},
			mustBeMasked: true,
		},
		{
			name:         "email with special characters",
			input:        []byte("email\nnlogin@domain.ru"),
			expected:     []byte("email\nSECMASKED"),
			comment:      "do not replace email",
			masks:        Mask{Re: kEMailRegExp, mode: modeReplace, ReplaceWord: "SECMASKED", Groups: []int{0}, MaxCount: 10},
			mustBeMasked: true,
		},
		{
			name:         "mask many values",
			input:        []byte("test 1 mask 2 mask 3 mask 4 end"),
			expected:     []byte("test 1 **** 2 **** 3 **** 4 end"),
			comment:      "can't mask many values",
			masks:        Mask{Re: "(mask)", mode: modeMask, Groups: []int{1}},
			mustBeMasked: true,
		},
		{
			name:         "mask many values with limit",
			input:        []byte("test 1 mask 2 mask 3 mask 4 end"),
			expected:     []byte("test 1 ** 2 ** 3 ** 4 end"),
			comment:      "can't mask many values with limit",
			masks:        Mask{Re: "(mask)", mode: modeMask, Groups: []int{1}, MaxCount: 2},
			mustBeMasked: true,
		},
		{
			name:         "mask many UTF-8 values",
			input:        []byte("test 1 Петя 2 Петя 3 Петя 4 end"),
			expected:     []byte("test 1 **** 2 **** 3 **** 4 end"),
			comment:      "can't mask many UTF-8 values",
			masks:        Mask{Re: "(Петя)", mode: modeMask, Groups: []int{1}},
			mustBeMasked: true,
		},
		{
			name:         "mask many UTF-8 values with limit",
			input:        []byte("test 1 Вася 2 Вася 3 Вася 4 end"),
			expected:     []byte("test 1 ** 2 ** 3 ** 4 end"),
			comment:      "can't mask many UTF-8 values with limit",
			masks:        Mask{Re: "(Вася)", mode: modeMask, Groups: []int{1}, MaxCount: 2},
			mustBeMasked: true,
		},
		{
			name:         "cut many values",
			input:        []byte("test 1 mask 2 mask 3 mask 4 end"),
			expected:     []byte("test 1  2  3  4 end"),
			comment:      "can't cut many values",
			masks:        Mask{Re: "(mask)", mode: modeCut, Groups: []int{1}},
			mustBeMasked: true,
		},
		{
			name:         "replace many values with short word",
			input:        []byte("test 1 mask 2 mask 3 mask 4 end"),
			expected:     []byte("test 1 ab 2 ab 3 ab 4 end"),
			comment:      "can't replace many values with short word",
			masks:        Mask{Re: "(mask)", mode: modeReplace, ReplaceWord: "ab", Groups: []int{1}},
			mustBeMasked: true,
		},
		{
			name:         "replace many values with long word",
			input:        []byte("test 1 mask 2 mask 3 mask 4 end"),
			expected:     []byte("test 1 qwerty 2 qwerty 3 qwerty 4 end"),
			comment:      "can't replace many values with long word",
			masks:        Mask{Re: "(mask)", mode: modeReplace, ReplaceWord: "qwerty", Groups: []int{1}},
			mustBeMasked: true,
		},
	}

	for _, tCase := range suits {
		t.Run(tCase.name, func(t *testing.T) {
			buf := make([]byte, 0, 2048)
			tCase.masks.Re_ = regexp.MustCompile(tCase.masks.Re)
			buf, masked := tCase.masks.maskValue(tCase.input, buf)
			assert.Equal(t, string(tCase.expected), string(buf), tCase.comment)
			assert.Equal(t, tCase.mustBeMasked, masked)
		})
	}
}

func TestMaskAddExtraField(t *testing.T) {
	input := `{"card":"5408-7430-0756-2004"}`
	key := "extra_key"
	val := "extra_val"
	expOutput := `{"card":"****-****-****-****","extra_key":"extra_val"}`

	root, err := insaneJSON.DecodeString(input)
	require.NoError(t, err)
	defer insaneJSON.Release(root)

	event := &pipeline.Event{Root: root}

	var plugin Plugin

	config := test.NewConfig(&Config{
		MaskAppliedField: key,
		MaskAppliedValue: val,
		SkipMismatched:   true,
		Masks: []Mask{
			{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3, 4}},
		},
	}, nil)
	plugin.Start(config, test.NewEmptyActionPluginParams())
	plugin.config.Masks[0].Re_ = regexp.MustCompile(plugin.config.Masks[0].Re)

	result := plugin.Do(event)
	assert.Equal(t, pipeline.ActionPass, result)
	assert.Equal(t, expOutput, event.Root.EncodeToString())
}

func TestGroupNumbers(t *testing.T) {
	suits := []struct {
		name     string
		input    *Mask
		expect   *Mask
		isFatal  bool
		fatalMsg string
		comment  string
	}{
		{
			name:    "simple test",
			input:   &Mask{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3}},
			expect:  &Mask{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3}},
			isFatal: false,
			comment: "mask successfully compiled",
		},
		{
			name:    "groups contains `zero`",
			input:   &Mask{Re: kDefaultCardRegExp, Groups: []int{0, 1, 2, 3}},
			expect:  &Mask{Re: kDefaultCardRegExp, Groups: []int{0}},
			isFatal: false,
			comment: "deleted all groups except zero",
		},
		{
			name:     "negative group number",
			input:    &Mask{Re: kDefaultCardRegExp, Groups: []int{-1}},
			expect:   &Mask{Re: kDefaultCardRegExp, Groups: []int{}},
			isFatal:  true,
			fatalMsg: "wrong group number",
			comment:  "fatal on negative group number",
		},
		{
			name:     "big value of group number",
			input:    &Mask{Re: kDefaultCardRegExp, Groups: []int{11}},
			expect:   &Mask{Re: kDefaultCardRegExp, Groups: []int{}},
			isFatal:  true,
			fatalMsg: "wrong group number",
			comment:  "fatal on checking group number",
		},
		{
			name:    "zero in group numbers",
			input:   &Mask{Re: kDefaultCardRegExp, Groups: []int{0}},
			expect:  &Mask{Re: kDefaultCardRegExp, Groups: []int{0}},
			isFatal: false,
			comment: "compiling success",
		},
		{
			name:     "replace mode and cut mode are both enabled",
			input:    &Mask{Re: kDefaultCardRegExp, Groups: []int{0}, ReplaceWord: "SECRET", CutValues: true},
			isFatal:  true,
			fatalMsg: "replace mode and cut mode are incompatible",
			comment:  "replace mode and cut mode are both enabled",
		},
		{
			name:    "replace mode enabled",
			input:   &Mask{Re: kDefaultCardRegExp, Groups: []int{0}, ReplaceWord: "SECRET"},
			expect:  &Mask{Re: kDefaultCardRegExp, Groups: []int{0}, ReplaceWord: "SECRET", mode: modeReplace},
			isFatal: false,
			comment: "replace mode enabled",
		},
		{
			name:    "cut mode enabled",
			input:   &Mask{Re: kDefaultCardRegExp, Groups: []int{0}, CutValues: true},
			expect:  &Mask{Re: kDefaultCardRegExp, Groups: []int{0}, CutValues: true, mode: modeCut},
			isFatal: false,
			comment: "cut mode enabled",
		},
		{
			name:    "mask mode enabled",
			input:   &Mask{Re: kDefaultCardRegExp, Groups: []int{0}},
			expect:  &Mask{Re: kDefaultCardRegExp, Groups: []int{0}, mode: modeMask},
			isFatal: false,
			comment: "mask mode enabled",
		},
		{
			name:     "error in expression",
			input:    &Mask{Re: "(err", Groups: []int{1}},
			expect:   &Mask{Re: kDefaultCardRegExp, Groups: []int{}},
			isFatal:  true,
			fatalMsg: "error on compiling regexp",
			comment:  "fatal on compiling regexp",
		},
		{
			name:     "big value of group number with zero first",
			input:    &Mask{Re: kDefaultCardRegExp, Groups: []int{0, 1, 2, 3, 4, 5}},
			isFatal:  true,
			fatalMsg: "there are many groups",
			comment:  "fatal error",
		},
		{
			name:     "big value of group number with zero last",
			input:    &Mask{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3, 4, 5, 0}},
			isFatal:  true,
			fatalMsg: "there are many groups",
			comment:  "fatal error",
		},
		{
			name:     "many value of group number",
			input:    &Mask{Re: kDefaultCardRegExp, Groups: []int{1, 2, 3, 4, 5}},
			isFatal:  true,
			fatalMsg: "there are many groups",
			comment:  "group 5 not exists in regex",
		},
		{
			name:     "wrong value of group number",
			input:    &Mask{Re: kDefaultCardRegExp, Groups: []int{6}},
			isFatal:  true,
			fatalMsg: "wrong group number",
			comment:  "group 6 not exists in regex",
		},
		{
			name:     "wrong negative value of group number",
			input:    &Mask{Re: kDefaultCardRegExp, Groups: []int{-6}},
			isFatal:  true,
			fatalMsg: "wrong group number",
			comment:  "group -6 not exists in regex",
		},
		{
			name:     "groups numbers not unique",
			input:    &Mask{Re: kDefaultCardRegExp, Groups: []int{1, 1, 1}},
			isFatal:  true,
			fatalMsg: "groups numbers must be unique",
			comment:  "not unique value",
		},
	}

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			if s.isFatal {
				assert.PanicsWithValue(t,
					s.fatalMsg,
					func() {
						compileMask(
							s.input,
							zap.NewNop().WithOptions(zap.WithFatalHook(zapcore.WriteThenPanic)),
						)
					},
					s.comment)
			} else {
				res := &Mask{
					Re:     s.input.Re,
					Groups: s.input.Groups,

					CutValues:   s.input.CutValues,
					ReplaceWord: s.input.ReplaceWord,
				}
				compileMask(res, zap.NewNop())
				assert.NotNil(t, res.Re_, s.comment)
				assert.Equal(t, res.Re, s.expect.Re, s.comment)
				assert.Equal(t, res.Groups, s.expect.Groups, s.comment)

				assert.Equal(t, res.CutValues, s.expect.CutValues)
				assert.Equal(t, res.ReplaceWord, s.expect.ReplaceWord)
				assert.Equal(t, res.mode, s.expect.mode, s.comment)
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
			input:    []string{`{"field1":"5679-0643-9766-5536"}`},
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
			name:     "email",
			input:    []string{`{"field1":"email login@domain.ru"}`},
			expected: []string{`{"field1":"email SECMASKED"}`},
			comment:  "email masked",
		},
		{
			name:     "card number with text",
			input:    []string{`{"field1":"authorization of card number 5679-0643-9766-5536 failed"}`},
			expected: []string{`{"field1":"authorization of card number ****-****-****-**** failed"}`},
			comment:  "only card number masked",
		},
		{
			name:     "ID&text&card",
			input:    []string{`{"field1":"Иванов Иван Иванович paid by card number 5679-0643-9766-5536"}`},
			expected: []string{`{"field1":"******************** paid by card number ****-****-****-****"}`},
			comment:  "only ID & card number masked",
		},
		{
			name:     "ID&text&2cards",
			input:    []string{`{"field1":"Иванов Иван Иванович have cards number 5679-0643-9766-5536, 3528-3889-3793-9946"}`},
			expected: []string{`{"field1":"******************** have cards number ****-****-****-****, ****-****-****-****"}`},
			comment:  "ID masked, two card numbers also masked",
		},
		{
			name: "ID&text&card pipeline",
			input: []string{
				`{"field1":"authorization of card number 5679-0643-9766-5536 failed"}`,
				`{"field2":"Simple event"}`,
				`{"field3":"Просто событие"}`,
				`{"field4":"Иванов Иван Иванович have cards number ****-****-****-****, ****-****-****-****"}`,
			},
			expected: []string{
				`{"field1":"authorization of card number ****-****-****-**** failed"}`,
				`{"field2":"Simple event"}`,
				`{"field3":"Просто событие"}`,
				`{"field4":"******************** have cards number ****-****-****-****, ****-****-****-****"}`,
			},
			comment: "only ID & card number masked",
		},
		{
			name: "special chars",
			input: []string{
				`{"field1":"email\\nlogin@domain.ru"}`,
				`{"field1":"email\nlogin@domain.ru"}`,
				`{"field1":"email\login@domain.ru"}`,
			},
			expected: []string{
				`{"field1":"email\\SECMASKED"}`,
				`{"field1":"email\nSECMASKED"}`,
				`{"field1":"email\\SECMASKED"}`,
			},
			comment: "mask values with special chars",
		},
	}

	config := test.NewConfig(&Config{
		SkipMismatched: true,
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
			{
				Re:          kEMailRegExp,
				Groups:      []int{0},
				ReplaceWord: "SECMASKED",
			},
		},
	}, nil)

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			sut, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(factory, config,
					pipeline.MatchModeAnd,
					nil,
					false))
			wg := sync.WaitGroup{}
			wg.Add(len(s.input))

			outEvents := make([]string, 0, len(s.expected))
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents = append(outEvents, e.Root.EncodeToString())
				wg.Done()
			})

			for _, in := range s.input {
				input.In(0, "test.log", test.NewOffset(0), []byte(in))
			}

			wg.Wait()
			sut.Stop()

			for i := range s.expected {
				assert.Equal(t, s.expected[i], outEvents[i], s.comment)
				assert.True(t, json.Valid([]byte(outEvents[i])))
			}
		})
	}
}

func TestWithEmptyRegex(t *testing.T) {
	suits := []struct {
		name     string
		input    []string
		expected []string
		comment  string
	}{
		{
			name:     "ID&card",
			input:    []string{`{"field1":"Индивидуальный предприниматель Иванов Иван Иванович"}`},
			expected: []string{`{"field1":"Индивидуальный предприниматель Иванов Иван Иванович","access_token_leaked":"personal_data_leak"}`},
			comment:  "Add field access_token_leaked",
		},
	}

	config := test.NewConfig(&Config{
		SkipMismatched: true,
		Masks: []Mask{
			{
				MatchRules: []matchrule.RuleSet{
					{
						Rules: []matchrule.Rule{
							{
								Values:          []string{"Индивидуальный предприниматель"},
								Mode:            matchrule.ModeContains,
								CaseInsensitive: false,
							},
						},
					},
				},
				AppliedField: "access_token_leaked",
				AppliedValue: "personal_data_leak",
				MetricName:   "sec_dataleak_predprinimatel",
				MetricLabels: []string{"service"},
			},
			{
				Re:     kDefaultCardRegExp,
				Groups: []int{1, 2, 3, 4},
			},
		},
	}, nil)

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			sut, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(factory, config,
					pipeline.MatchModeAnd,
					nil,
					false))
			wg := sync.WaitGroup{}
			wg.Add(len(s.input))

			outEvents := make([]string, 0, len(s.expected))
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents = append(outEvents, e.Root.EncodeToString())
				wg.Done()
			})

			for _, in := range s.input {
				input.In(0, "test.log", test.NewOffset(0), []byte(in))
			}

			wg.Wait()
			sut.Stop()

			for i := range s.expected {
				assert.Equal(t, s.expected[i], outEvents[i], s.comment)
			}
		})
	}
}

//nolint:funlen
func TestPluginWithComplexMasks(t *testing.T) {
	suits := []struct {
		name         string
		masks        []Mask
		metricName   string
		metricLabels []string
		input        []string
		expected     []string
		comment      string
	}{
		{
			name: "single mask w single ruleset & re w replace",
			masks: []Mask{
				{
					MatchRules: []matchrule.RuleSet{
						{
							Cond: matchrule.CondAnd,
							Rules: []matchrule.Rule{
								{
									Values:          []string{"prefix1", "1prefix"},
									Mode:            matchrule.ModePrefix,
									CaseInsensitive: true,
								},
								{
									Values:          []string{"suffix1", "1suffix"},
									Mode:            matchrule.ModeSuffix,
									CaseInsensitive: true,
								},
							},
						},
					},
					Re:           `(to\_mask)`,
					Groups:       []int{0},
					ReplaceWord:  "REPLACED",
					AppliedField: "mask_field",
					AppliedValue: "mask_value",
					MetricName:   "test_mask_metric",
					MetricLabels: []string{"service"},
				},
			},
			metricName:   "test_metric",
			metricLabels: []string{"service"},
			input: []string{
				`{"field1":"prefix1 to_mask suffix1","service":"test"}`,
				`{"field1":"1prefix to_mask 1suffix"}`,
				`{"field1":"prefix1 test suffix1"}`,
				`{"field1":"prefix2 to_mask suffix2"}`,
			},
			expected: []string{
				`{"field1":"prefix1 REPLACED suffix1","service":"test","mask_field":"mask_value"}`,
				`{"field1":"1prefix REPLACED 1suffix","mask_field":"mask_value"}`,
				`{"field1":"prefix1 test suffix1"}`,
				`{"field1":"prefix2 to_mask suffix2"}`,
			},
			comment: "single mask with single ruleset and regex with replace word",
		},
		{
			name: "single mask w multi ruleset & single re w replace",
			masks: []Mask{
				{
					MatchRules: []matchrule.RuleSet{
						{
							Cond: matchrule.CondAnd,
							Rules: []matchrule.Rule{
								{
									Values:          []string{"prefix1", "1prefix"},
									Mode:            matchrule.ModePrefix,
									CaseInsensitive: true,
								},
								{
									Values:          []string{"suffix1", "1suffix"},
									Mode:            matchrule.ModeSuffix,
									CaseInsensitive: true,
								},
							},
						},
						{
							Cond: matchrule.CondAnd,
							Rules: []matchrule.Rule{
								{
									Values:          []string{"prefix2", "2prefix"},
									Mode:            matchrule.ModePrefix,
									CaseInsensitive: true,
								},
								{
									Values:          []string{"suffix2", "2suffix"},
									Mode:            matchrule.ModeSuffix,
									CaseInsensitive: true,
								},
							},
						},
					},
					Re:           `(to\_mask)`,
					Groups:       []int{0},
					ReplaceWord:  "REPLACED",
					AppliedField: "mask_field",
					AppliedValue: "mask_value",
				},
			},
			input: []string{
				`{"field1":"prefix1 to_mask suffix1"}`,
				`{"field1":"1prefix to_mask 1suffix"}`,
				`{"field1":"prefix1 test suffix1"}`,
				`{"field1":"prefix2 to_mask suffix2"}`,
			},
			expected: []string{
				`{"field1":"prefix1 REPLACED suffix1","mask_field":"mask_value"}`,
				`{"field1":"1prefix REPLACED 1suffix","mask_field":"mask_value"}`,
				`{"field1":"prefix1 test suffix1"}`,
				`{"field1":"prefix2 REPLACED suffix2","mask_field":"mask_value"}`,
			},
			comment: "single mask with multi rulesets and regex with replace word",
		},
		{
			name: "single mask w single ruleset & wo re",
			masks: []Mask{
				{
					MatchRules: []matchrule.RuleSet{
						{
							Cond: matchrule.CondAnd,
							Rules: []matchrule.Rule{
								{
									Values:          []string{"prefix1", "1prefix"},
									Mode:            matchrule.ModePrefix,
									CaseInsensitive: true,
								},
								{
									Values:          []string{"suffix1", "1suffix"},
									Mode:            matchrule.ModeSuffix,
									CaseInsensitive: true,
								},
							},
						},
					},
					AppliedField: "mask_field",
					AppliedValue: "mask_value",
				},
			},
			input: []string{
				`{"field1":"prefix1 to_mask suffix1"}`,
				`{"field1":"1prefix to_mask 1suffix"}`,
				`{"field1":"prefix1 test suffix1"}`,
				`{"field1":"prefix2 to_mask suffix2"}`,
			},
			expected: []string{
				`{"field1":"prefix1 to_mask suffix1","mask_field":"mask_value"}`,
				`{"field1":"1prefix to_mask 1suffix","mask_field":"mask_value"}`,
				`{"field1":"prefix1 test suffix1","mask_field":"mask_value"}`,
				`{"field1":"prefix2 to_mask suffix2"}`,
			},
			comment: "single mask with single ruleset and without regex",
		},
	}

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			config := test.NewConfig(&Config{
				SkipMismatched:      true,
				Masks:               s.masks,
				AppliedMetricName:   s.metricName,
				AppliedMetricLabels: s.metricLabels,
			}, nil)
			sut, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(factory, config,
					pipeline.MatchModeAnd,
					nil,
					false))
			wg := sync.WaitGroup{}
			wg.Add(len(s.input))

			outEvents := make([]string, 0, len(s.expected))
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents = append(outEvents, e.Root.EncodeToString())
				wg.Done()
			})

			for _, in := range s.input {
				input.In(0, "test.log", test.NewOffset(0), []byte(in))
			}

			wg.Wait()
			sut.Stop()

			for i := range s.expected {
				assert.Equal(t, s.expected[i], outEvents[i], s.comment)
			}
		})
	}
}

func TestIgnoreProcessFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		ignoreFields  []string
		processFields []string
		masks         []Mask
		input         []string
		expected      []string
		comment       string
	}{
		{
			name:         "global_ignore_fields_flat_single_mask_ok",
			ignoreFields: []string{"f3"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED",
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"another test val","f3":"more test val"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests","f2":"another test val","f3":"more test val"}`,
				`"some test-string not-json"`,
				`["testarrval"]`,
			},
			expected: []string{
				`{"f1":"some REPLACED val","f2":"another REPLACED val","f3":"more test val"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some REPLACED val REPLACED more REPLACEDs","f2":"another REPLACED val","f3":"more test val"}`,
				`"some REPLACED-string not-json"`,
				`["REPLACEDarrval"]`,
			},
			comment: "global ignore fields flat with single mask no error",
		},
		{
			name:          "global_process_fields_flat_single_mask_ok",
			processFields: []string{"f3"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED",
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"another test val","f3":"more test val"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests","f2":"another test val","f3":"more test val test testtest atestb"}`,
				`"some test-string not-json"`,
				`["testarrval"]`,
			},
			expected: []string{
				`{"f1":"some test val","f2":"another test val","f3":"more REPLACED val"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests","f2":"another test val","f3":"more REPLACED val REPLACED REPLACEDREPLACED aREPLACEDb"}`,
				`"some test-string not-json"`,
				`["testarrval"]`,
			},
			comment: "global process fields flat with single mask no error",
		},
		{
			name:         "global_ignore_fields_flat_multi_mask_ok",
			ignoreFields: []string{"f3"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED1",
				},
				{
					Re:          "(tst)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED2",
				},
				{
					Re:          "(tesst)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED3",
				},
			},
			input: []string{
				`{"f1":"some test tst tesst val","f2":"another tesst tst test val","f3":"more test val"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some tst tst tesst tesst test val test more tests","f2":"another test val tst atestb test ctstd","f3":"more test val"}`,
				`"some test-string not-json"`,
				`["testarrval"]`,
			},
			expected: []string{
				`{"f1":"some REPLACED1 REPLACED2 REPLACED3 val","f2":"another REPLACED3 REPLACED2 REPLACED1 val","f3":"more test val"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some REPLACED2 REPLACED2 REPLACED3 REPLACED3 REPLACED1 val REPLACED1 more REPLACED1s","f2":"another REPLACED1 val REPLACED2 aREPLACED1b REPLACED1 cREPLACED2d","f3":"more test val"}`,
				`"some REPLACED1-string not-json"`,
				`["REPLACED1arrval"]`,
			},
			comment: "global ignore fields flat with multiple masks no error",
		},
		{
			name:          "global_process_fields_flat_multi_mask_ok",
			processFields: []string{"f3"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED1",
				},
				{
					Re:          "(tst)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED2",
				},
				{
					Re:          "(tesst)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED3",
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"another test val","f3":"more test tst tesst val"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests","f2":"another test val","f3":"more test val test tsttessttest testtest atestb ctesstd"}`,
				`"some test-string not-json"`,
				`["testarrval"]`,
			},
			expected: []string{
				`{"f1":"some test val","f2":"another test val","f3":"more REPLACED1 REPLACED2 REPLACED3 val"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests","f2":"another test val","f3":"more REPLACED1 val REPLACED1 REPLACED2REPLACED3REPLACED1 REPLACED1REPLACED1 aREPLACED1b cREPLACED3d"}`,
				`"some test-string not-json"`,
				`["testarrval"]`,
			},
			comment: "global process fields flat with multiple masks no error",
		},
		{
			name: "inmask_ignore_fields_flat_two_mask_ok",
			masks: []Mask{
				{
					Re:           "(test)",
					Groups:       []int{0},
					ReplaceWord:  "REPLACED1",
					IgnoreFields: []string{"f3"},
				},
				{
					Re:          "(tst)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED2",
				},
			},
			input: []string{
				`{"f1":"some test tst tesst val","f2":"another tesst tst test val","f3":"more test val tst"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some tst tst tesst tesst test val test more tests","f2":"another test val tst atestb test ctstd","f3":"more test val tsttesttst"}`,
				`"some test-string not-json tst"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":"some REPLACED1 REPLACED2 tesst val","f2":"another tesst REPLACED2 REPLACED1 val","f3":"more test val REPLACED2"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some REPLACED2 REPLACED2 tesst tesst REPLACED1 val REPLACED1 more REPLACED1s","f2":"another REPLACED1 val REPLACED2 aREPLACED1b REPLACED1 cREPLACED2d","f3":"more test val REPLACED2testREPLACED2"}`,
				`"some REPLACED1-string not-json REPLACED2"`,
				`["REPLACED1arrvalREPLACED2"]`,
			},
			comment: "mask-specific ignore fields flat with two masks no error",
		},
		{
			name: "inmask_process_fields_flat_two_mask_ok",
			masks: []Mask{
				{
					Re:            "(test)",
					Groups:        []int{0},
					ReplaceWord:   "REPLACED1",
					ProcessFields: []string{"f3"},
				},
				{
					Re:          "(tst)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED2",
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"tst another test val","f3":"more test tst tesst val"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":"another test val","f3":"more test val test tsttessttest testtest atestb ctesstd"}`,
				`"some test-string not-json tst"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":"some test val","f2":"REPLACED2 another test val","f3":"more REPLACED1 REPLACED2 tesst val"}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests REPLACED2","f2":"another test val","f3":"more REPLACED1 val REPLACED1 REPLACED2tesstREPLACED1 REPLACED1REPLACED1 aREPLACED1b ctesstd"}`,
				`"some test-string not-json REPLACED2"`,
				`["testarrvalREPLACED2"]`,
			},
			comment: "mask-specific process fields flat with two masks no error",
		},
		{
			name:         "global_ignore_fields_nested_single_mask_ok_1",
			ignoreFields: []string{"f3"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED",
				},
			},
			input: []string{
				`{"f1":{"f5":"some test val"},"f2":"another test val","f3":{"f4":"more test val","ff4":"testtestval"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":{"f6":"some test val test more tests"},"f2":"another test val","f3":{"f7":"more test val"}}`,
				`"some test-string not-json"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":{"f5":"some REPLACED val"},"f2":"another REPLACED val","f3":{"f4":"more test val","ff4":"testtestval"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":{"f6":"some REPLACED val REPLACED more REPLACEDs"},"f2":"another REPLACED val","f3":{"f7":"more test val"}}`,
				`"some REPLACED-string not-json"`,
				`["REPLACEDarrvaltst"]`,
			},
			comment: "global ignore fields nested with single mask no error",
		},
		{
			name:         "global_ignore_fields_nested_single_mask_ok_2",
			ignoreFields: []string{"f3.f4"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED",
				},
			},
			input: []string{
				`{"f1":{"f5":"some test val"},"f2":"another test val","f3":{"f4":"more test val","ff4":"testtestval"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":{"f6":"some test val test more tests"},"f2":"another test val","f3":{"f7":"more test val","f8":["testarrval"]}}`,
				`"some test-string not-json"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":{"f5":"some REPLACED val"},"f2":"another REPLACED val","f3":{"f4":"more test val","ff4":"REPLACEDREPLACEDval"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":{"f6":"some REPLACED val REPLACED more REPLACEDs"},"f2":"another REPLACED val","f3":{"f7":"more REPLACED val","f8":["REPLACEDarrval"]}}`,
				`"some REPLACED-string not-json"`,
				`["REPLACEDarrvaltst"]`,
			},
			comment: "global ignore fields nested with single mask no error",
		},
		{
			name:         "global_ignore_fields_nested_single_mask_ok_3",
			ignoreFields: []string{"f1.1"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED",
				},
			},
			input: []string{
				`{"f1":[{"f101":"test val","f102":"another test val"},{"f111":"again test val"}]}`,
				`"some test-string not-json"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":[{"f101":"REPLACED val","f102":"another REPLACED val"},{"f111":"again test val"}]}`,
				`"some REPLACED-string not-json"`,
				`["REPLACEDarrvaltst"]`,
			},
			comment: "global ignore fields nested with single mask no error",
		},
		{
			name:          "global_process_fields_nested_single_mask_ok_1",
			processFields: []string{"f3"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED",
				},
			},
			input: []string{
				`{"f1":{"f5":"some test val"},"f2":"another test val","f3":{"f4":"more test val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":{"f6":"some test val test more tests"},"f2":"another test val","f3":{"f7":"more test val"}}`,
				`"some test-string not-json"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":{"f5":"some test val"},"f2":"another test val","f3":{"f4":"more REPLACED val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":{"f6":"some test val test more tests"},"f2":"another test val","f3":{"f7":"more REPLACED val"}}`,
				`"some test-string not-json"`,
				`["testarrvaltst"]`,
			},
			comment: "global process fields nested with single mask no error",
		},
		{
			name:          "global_process_fields_nested_single_mask_ok_2",
			processFields: []string{"f3.f4"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED",
				},
			},
			input: []string{
				`{"f1":{"f5":"some test val"},"f2":"another test val","f3":{"f4":"more test val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":{"f6":"some test val test more tests"},"f2":"another test val","f3":{"f7":"more test val"}}`,
				`{"f1":{"f5":"some test val"},"f2":"another test val","f3":{"f4":{"f8":"more test val"}}}`,
				`"some test-string not-json"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":{"f5":"some test val"},"f2":"another test val","f3":{"f4":"more REPLACED val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":{"f6":"some test val test more tests"},"f2":"another test val","f3":{"f7":"more test val"}}`,
				`{"f1":{"f5":"some test val"},"f2":"another test val","f3":{"f4":{"f8":"more REPLACED val"}}}`,
				`"some test-string not-json"`,
				`["testarrvaltst"]`,
			},
			comment: "global process fields nested with single mask no error",
		},
		{
			name:          "global_process_fields_nested_single_mask_ok_3",
			processFields: []string{"f1.1"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED",
				},
			},
			input: []string{
				`{"f1":[{"f101":"test val","f102":"another test val"},{"f111":"again test val"}]}`,
				`"some test-string not-json"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":[{"f101":"test val","f102":"another test val"},{"f111":"again REPLACED val"}]}`,
				`"some test-string not-json"`,
				`["testarrvaltst"]`,
			},
			comment: "global process fields nested with single mask no error",
		},
		{
			name: "inmask_ignore_fields_nested_two_mask_ok_1",
			masks: []Mask{
				{
					Re:           "(test)",
					Groups:       []int{0},
					ReplaceWord:  "REPLACED1",
					IgnoreFields: []string{"f3"},
				},
				{
					Re:          "(tst)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED2",
				},
			},
			input: []string{
				`{"f1":{"f6":"some test val"},"f2":"tst another test val","f3":{"f4":"more test tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":["another test val"],"f3":{"f5":"more test val test tsttessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":{"f6":"some REPLACED1 val"},"f2":"REPLACED2 another REPLACED1 val","f3":{"f4":"more test REPLACED2 tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some REPLACED1 val REPLACED1 more REPLACED1s REPLACED2","f2":["another REPLACED1 val"],"f3":{"f5":"more test val test REPLACED2tessttest testtest atestb ctesstd"}}`,
				`"some REPLACED1-string not-json REPLACED2"`,
				`["REPLACED1arrvalREPLACED2"]`,
			},
			comment: "mask-specific ignore fields nested with two masks no error",
		},
		{
			name: "inmask_ignore_fields_nested_two_mask_ok_2",
			masks: []Mask{
				{
					Re:           "(test)",
					Groups:       []int{0},
					ReplaceWord:  "REPLACED1",
					IgnoreFields: []string{"f3.f4"},
				},
				{
					Re:          "(tst)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED2",
				},
			},
			input: []string{
				`{"f1":{"f6":"some test val"},"f2":"tst another test val","f3":{"f4":"more test tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":["another test val"],"f3":{"f5":"more test val test tsttessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":{"f6":"some REPLACED1 val"},"f2":"REPLACED2 another REPLACED1 val","f3":{"f4":"more test REPLACED2 tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some REPLACED1 val REPLACED1 more REPLACED1s REPLACED2","f2":["another REPLACED1 val"],"f3":{"f5":"more REPLACED1 val REPLACED1 REPLACED2tesstREPLACED1 REPLACED1REPLACED1 aREPLACED1b ctesstd"}}`,
				`"some REPLACED1-string not-json REPLACED2"`,
				`["REPLACED1arrvalREPLACED2"]`,
			},
			comment: "mask-specific ignore fields nested with two masks no error",
		},
		{
			name: "inmask_process_fields_nested_two_mask_ok_1",
			masks: []Mask{
				{
					Re:            "(test)",
					Groups:        []int{0},
					ReplaceWord:   "REPLACED1",
					ProcessFields: []string{"f3"},
				},
				{
					Re:          "(tst)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED2",
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"tst another test val","f3":{"f4":"more test tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":"another test val","f3":{"f5":"more test val test tsttessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":"some test val","f2":"REPLACED2 another test val","f3":{"f4":"more REPLACED1 REPLACED2 tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests REPLACED2","f2":"another test val","f3":{"f5":"more REPLACED1 val REPLACED1 REPLACED2tesstREPLACED1 REPLACED1REPLACED1 aREPLACED1b ctesstd"}}`,
				`"some test-string not-json REPLACED2"`,
				`["testarrvalREPLACED2"]`,
			},
			comment: "mask-specific process fields nested with two masks no error",
		},
		{
			name: "inmask_process_fields_nested_two_mask_ok_2",
			masks: []Mask{
				{
					Re:            "(test)",
					Groups:        []int{0},
					ReplaceWord:   "REPLACED1",
					ProcessFields: []string{"f3.f4"},
				},
				{
					Re:          "(tst)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED2",
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"tst another test val","f3":{"f4":"more test tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":"another test val","f3":{"f5":"more test val test tsttessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":"some test val","f2":"REPLACED2 another test val","f3":{"f4":"more REPLACED1 REPLACED2 tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests REPLACED2","f2":"another test val","f3":{"f5":"more test val test REPLACED2tessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json REPLACED2"`,
				`["testarrvalREPLACED2"]`,
			},
			comment: "mask-specific process fields nested with two masks no error",
		},
		{
			name:         "global_inmask_ignore_fields_nested_multiple_mask_ok_1",
			ignoreFields: []string{"f1"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED1",
				},
				{
					Re:           "(tst)",
					Groups:       []int{0},
					ReplaceWord:  "REPLACED2",
					IgnoreFields: []string{"f3"},
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"tst another test val","f3":{"f4":"more test tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":"another test val","f3":{"f5":"more test val test tsttessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":"some test val","f2":"REPLACED2 another REPLACED1 val","f3":{"f4":"more REPLACED1 tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests REPLACED2","f2":"another REPLACED1 val","f3":{"f5":"more REPLACED1 val REPLACED1 tsttesstREPLACED1 REPLACED1REPLACED1 aREPLACED1b ctesstd"}}`,
				`"some REPLACED1-string not-json REPLACED2"`,
				`["REPLACED1arrvalREPLACED2"]`,
			},
			comment: "mask-specific ignore fields nested with two masks no error",
		},
		{
			name:          "global_inmask_process_fields_nested_multiple_mask_ok_1",
			processFields: []string{"f1"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED1",
				},
				{
					Re:            "(tst)",
					Groups:        []int{0},
					ReplaceWord:   "REPLACED2",
					ProcessFields: []string{"f3"},
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"tst another test val","f3":{"f4":"more test tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":"another test val","f3":{"f5":"more test val test tsttessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":"some REPLACED1 val","f2":"tst another test val","f3":{"f4":"more test REPLACED2 tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some REPLACED1 val REPLACED1 more REPLACED1s tst","f2":"another test val","f3":{"f5":"more test val test REPLACED2tessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltst"]`,
			},
			comment: "mask-specific process fields nested with two masks no error",
		},
		{
			name:          "global_inmask_ignore_process_fields_nested_multiple_mask_ok_1",
			processFields: []string{"f1"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED1",
				},
				{
					Re:           "(tst)",
					Groups:       []int{0},
					ReplaceWord:  "REPLACED2",
					IgnoreFields: []string{"f3"},
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"tst another test val","f3":{"f4":"more test tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":"another test val","f3":{"f5":"more test val test tsttessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":"some REPLACED1 val","f2":"REPLACED2 another test val","f3":{"f4":"more test tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some REPLACED1 val REPLACED1 more REPLACED1s REPLACED2","f2":"another test val","f3":{"f5":"more test val test tsttessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json REPLACED2"`,
				`["testarrvalREPLACED2"]`,
			},
			comment: "mask-specific ignore fields nested with two masks no error",
		},
		{
			name:         "global_inmask_process_ignore_fields_nested_multiple_mask_ok_1",
			ignoreFields: []string{"f1"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED1",
				},
				{
					Re:            "(tst)",
					Groups:        []int{0},
					ReplaceWord:   "REPLACED2",
					ProcessFields: []string{"f3"},
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"tst another test val","f3":{"f4":"more test tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":"another test val","f3":{"f5":"more test val test tsttessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltst"]`,
			},
			expected: []string{
				`{"f1":"some test val","f2":"tst another REPLACED1 val","f3":{"f4":"more REPLACED1 REPLACED2 tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":"another REPLACED1 val","f3":{"f5":"more REPLACED1 val REPLACED1 REPLACED2tesstREPLACED1 REPLACED1REPLACED1 aREPLACED1b ctesstd"}}`,
				`"some REPLACED1-string not-json tst"`,
				`["REPLACED1arrvaltst"]`,
			},
			comment: "mask-specific ignore fields nested with two masks no error",
		},
		{
			name:         "complex_global_inmask_mixed_case_ok_1",
			ignoreFields: []string{"f1", "f2.f3"},
			masks: []Mask{
				{
					Re:          "(test)",
					Groups:      []int{0},
					ReplaceWord: "REPLACED1",
				},
				{
					Re:            "(tst)",
					Groups:        []int{0},
					ReplaceWord:   "REPLACED2",
					ProcessFields: []string{"f3.f5"},
				},
				{
					Re:           "(tesst)",
					Groups:       []int{0},
					ReplaceWord:  "REPLACED3",
					IgnoreFields: []string{"f3.f4"},
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"tst another test val","f3":{"f4":"more test tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":"another test val","f3":{"f5":"more test val test tsttessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltsttesst"]`,
			},
			expected: []string{
				`{"f1":"some test val","f2":"tst another REPLACED1 val","f3":{"f4":"more REPLACED1 tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":"another REPLACED1 val","f3":{"f5":"more REPLACED1 val REPLACED1 REPLACED2REPLACED3REPLACED1 REPLACED1REPLACED1 aREPLACED1b cREPLACED3d"}}`,
				`"some REPLACED1-string not-json tst"`,
				`["REPLACED1arrvaltstREPLACED3"]`,
			},
			comment: "mask-specific ignore fields nested with two masks no error",
		},
		{
			name:         "complex_global_inmask_mixed_case_ok_1",
			ignoreFields: []string{"f1", "f2.f3"},
			masks: []Mask{
				{
					Re:            "(test)",
					Groups:        []int{0},
					ReplaceWord:   "REPLACED1",
					ProcessFields: []string{"f1"},
				},
				{
					Re:            "(tst)",
					Groups:        []int{0},
					ReplaceWord:   "REPLACED2",
					ProcessFields: []string{"f3.f5"},
				},
				{
					Re:            "(tesst)",
					Groups:        []int{0},
					ReplaceWord:   "REPLACED3",
					ProcessFields: []string{"f3.f4"},
				},
			},
			input: []string{
				`{"f1":"some test val","f2":"tst another test val","f3":{"f4":"more test tst tesst val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some test val test more tests tst","f2":"another test val","f3":{"f5":"more test val test tsttessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltsttesst"]`,
			},
			expected: []string{
				`{"f1":"some REPLACED1 val","f2":"tst another test val","f3":{"f4":"more test tst REPLACED3 val"}}`,
				`{"f1":"some val","f2":"another val","f3":"more val"}`,
				`{"f1":"some REPLACED1 val REPLACED1 more REPLACED1s tst","f2":"another test val","f3":{"f5":"more test val test REPLACED2tessttest testtest atestb ctesstd"}}`,
				`"some test-string not-json tst"`,
				`["testarrvaltsttesst"]`,
			},
			comment: "mask-specific ignore fields nested with two masks no error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			config := test.NewConfig(&Config{
				SkipMismatched: true,
				Masks:          tt.masks,
				IgnoreFields:   tt.ignoreFields,
				ProcessFields:  tt.processFields,
			}, nil)
			p, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(factory, config,
					pipeline.MatchModeAnd,
					nil,
					false))
			wg := sync.WaitGroup{}
			wg.Add(len(tt.input))

			outEvents := make([]string, 0, len(tt.expected))
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents = append(outEvents, e.Root.EncodeToString())
				wg.Done()
			})

			for _, in := range tt.input {
				input.In(0, "test.log", test.NewOffset(0), []byte(in))
			}

			wg.Wait()
			p.Stop()

			for i := range tt.expected {
				assert.Equal(t, tt.expected[i], outEvents[i], tt.comment)
			}
		})
	}
}
