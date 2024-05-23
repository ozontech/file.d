package mask

import (
	"encoding/json"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"
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
			expected:     []byte("ab.cd.efgh"),
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
			masks:        Mask{Re: kDefaultIDRegExp, Groups: []int{0}, ReplaceWord: "***MASKED***"},
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
			masks:        Mask{Re: kEMailRegExp, ReplaceWord: "SECMASKED", Groups: []int{0}, MaxCount: 10},
			mustBeMasked: true,
		},
		{
			name:         "email with special characters",
			input:        []byte("email\nnlogin@domain.ru"),
			expected:     []byte("email\nSECMASKED"),
			comment:      "do not replace email",
			masks:        Mask{Re: kEMailRegExp, ReplaceWord: "SECMASKED", Groups: []int{0}, MaxCount: 10},
			mustBeMasked: true,
		},
	}

	var plugin Plugin

	for _, tCase := range suits {
		t.Run(tCase.name, func(t *testing.T) {
			buf := make([]byte, 0, 2048)
			tCase.masks.Re_ = regexp.MustCompile(tCase.masks.Re)
			buf, masked := plugin.maskValue(&tCase.masks, tCase.input, buf)
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
				}
				compileMask(res, zap.NewNop())
				assert.NotNil(t, res.Re_, s.comment)
				assert.Equal(t, res.Re, s.expect.Re, s.comment)
				assert.Equal(t, res.Groups, s.expect.Groups, s.comment)
			}
		})
	}
}

//nolint:funlen
func TestGetValueNodes(t *testing.T) {
	suits := []struct {
		name        string
		input       string
		fieldPaths  [][]string
		isWhitelist bool
		expected    []string
		comment     string
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
			name:  "test with ignored field",
			input: `{"name1":"value1", "name2":"value2", "ignored_field":"some"}`,
			fieldPaths: [][]string{
				{"ignored_field"},
			},
			isWhitelist: false,
			expected:    []string{"value1", "value2"},
			comment:     "skip ignored_field",
		},
		{
			name:  "test with processed field",
			input: `{"name1":"value1", "name2":"value2", "processed_field":"some"}`,
			fieldPaths: [][]string{
				{"processed_field"},
			},
			isWhitelist: true,
			expected:    []string{"some"},
			comment:     "skip all fields except processed_field",
		},
		{
			name: "test with ignored nested field 1",
			input: `{
				"name1":"value1",
				"name2":"value2",
				"nested": {
					"ignored_field":"some",
					"name3":"value3"
				}
			}`,
			fieldPaths: [][]string{
				{"nested", "ignored_field"},
			},
			isWhitelist: false,
			expected:    []string{"value1", "value2", "value3"},
			comment:     "skip nested ignored_field",
		},
		{
			name: "test with ignored nested field 2",
			input: `{
				"name1":"value1",
				"name2":"value2",
				"nested": {
					"ignored1":"some1",
					"ignored2":"some2"
				}
			}`,
			fieldPaths: [][]string{
				{"nested"},
			},
			isWhitelist: false,
			expected:    []string{"value1", "value2"},
			comment:     "skip nested ignored_field",
		},
		{
			name: "test with processed nested field 1",
			input: `{
				"name1":"value1",
				"name2":"value2",
				"nested": {
					"processed_field":"some",
					"name3": "value3"
				}
			}`,
			fieldPaths: [][]string{
				{"nested", "processed_field"},
			},
			isWhitelist: true,
			expected:    []string{"some"},
			comment:     "skip all fields except nested processed_field",
		},
		{
			name: "test with processed nested field 1",
			input: `{
				"name1":"value1",
				"name2":"value2",
				"nested": {
					"processed1":"some1",
					"processed2":"some2"
				}
			}`,
			fieldPaths: [][]string{
				{"nested"},
			},
			isWhitelist: true,
			expected:    []string{"some1", "some2"},
			comment:     "skip all fields except nested processed_field",
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
			require.NoError(t, err)
			defer insaneJSON.Release(root)

			nodes := getValueNodes(root.Node, nil, s.fieldPaths, s.isWhitelist)
			require.Equal(t, len(nodes), len(s.expected), s.comment)
			for i := range nodes {
				assert.Equal(t, s.expected[i], nodes[i].AsString(), s.comment)
			}
		})
	}
}

//nolint:funlen
func TestGetAllValueNodes(t *testing.T) {
	suits := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "one string",
			input:    `"abc"`,
			expected: []string{"abc"},
		},
		{
			name:     "one number",
			input:    `123`,
			expected: []string{"123"},
		},
		{
			name:     "one boolean",
			input:    `true`,
			expected: []string{"true"},
		},
		{
			name:     "one null",
			input:    `null`,
			expected: []string{"null"},
		},
		{
			name:     "simple array",
			input:    `["abc", 123, true, null]`,
			expected: []string{"abc", "123", "true", "null"},
		},
		{
			name:     "nested arrays",
			input:    `[[], ["abc", 123, [true]], [[[], [null]]]]`,
			expected: []string{"abc", "123", "true", "null"},
		},
		{
			name:     "simple object",
			input:    `{"name1":"abc", "name2":123, "name3":true, "name4":null}`,
			expected: []string{"abc", "123", "true", "null"},
		},
		{
			name:     "nested objects",
			input:    `{"f1": {"some":"abc", "f2": {"n":123, "flag":true, "f3": {"name":null}}}}`,
			expected: []string{"abc", "123", "true", "null"},
		},
		{
			name:     "array and object",
			input:    `{"f1": ["abc", {"name2":123, "name3":true}], "name": {"name":null}}`,
			expected: []string{"abc", "123", "true", "null"},
		},
	}

	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			root, err := insaneJSON.DecodeString(s.input)
			require.NoError(t, err)
			defer insaneJSON.Release(root)

			nodes := getAllValueNodes(root.Node, nil)
			require.Equal(t, len(nodes), len(s.expected))
			for i := range nodes {
				assert.Equal(t, s.expected[i], nodes[i].AsString())
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
				input.In(0, "test.log", 0, []byte(in))
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
				input.In(0, "test.log", 0, []byte(in))
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
				input.In(0, "test.log", 0, []byte(in))
			}

			wg.Wait()
			sut.Stop()

			for i := range s.expected {
				assert.Equal(t, s.expected[i], outEvents[i], s.comment)
			}
		})
	}
}

func createBenchInputString() []byte {
	matchable := `{"field1":"Иванов Иван Иванович c картой 4445-2222-3333-4444 встал не с той ноги"}`
	unmatchable := `{"field1":"Просто строка которая не заменяется"}`
	matchableCoeff := 0.1 // percentage of matchable input
	totalCount := 50
	matchableCount := int(float64(totalCount) * matchableCoeff)
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
	var plugin Plugin
	input := createBenchInputString()
	re := regexp.MustCompile(kDefaultCardRegExp)
	grp := []int{0, 1, 2, 3}
	mask := Mask{
		Re_:    re,
		Groups: grp,
	}
	buf := make([]byte, 0, 2048)
	for i := 0; i < b.N; i++ {
		buf, _ = plugin.maskValue(&mask, input, buf)
	}
}
