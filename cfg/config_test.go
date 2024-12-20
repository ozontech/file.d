package cfg

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func NewTestConfig(name string) *Config {
	return NewConfigFromFile([]string{"../testdata/config/" + name})
}

func TestSimple(t *testing.T) {
	c := NewTestConfig("e2e.yaml")

	assert.NotNil(t, c, "config loading should't return nil")

	assert.Equal(t, 1, len(c.Pipelines), "pipelines count isn't match")
}

type intDefault struct {
	T int `default:"5"`
}

type strRequired struct {
	T string `required:"true"`
}

type strDefault struct {
	T string `default:"sync"`
}

type boolDefault struct {
	T bool `default:"true"`
}

type PersistenceMode byte

const (
	PersistenceModeAsync PersistenceMode = iota
	PersistenceModeSync
)

type strOptions struct {
	T  string `default:"async" options:"async|sync"`
	T_ PersistenceMode
}

type strExpression struct {
	T  string `parse:"expression"`
	T_ int
}

type strDataUnit struct {
	T  string `parse:"data_unit"`
	T_ uint
}

type hierarchyChild struct {
	T string `required:"true"`
}

type hierarchy struct {
	T     string         `default:"sync"`
	Child hierarchyChild `child:"true"`
}

type sliceChild struct {
	Value string `default:"child"`
}

func (s *sliceChild) UnmarshalJSON(raw []byte) error {
	SetDefaultValues(s)
	var childPtr struct {
		Value *string
	}

	if err := json.Unmarshal(raw, &childPtr); err != nil {
		return err
	}

	if childPtr.Value != nil {
		s.Value = *childPtr.Value
	}

	return nil
}

type sliceStruct struct {
	Value  string       `default:"parent"`
	Childs []sliceChild `default:"" slice:"true"`
}

type strBase8 struct {
	T  string `default:"0666" parse:"base8"`
	T_ int64
}

func TestParseRequiredOk(t *testing.T) {
	s := &strRequired{T: "some_value"}
	err := Parse(s, nil)

	assert.NoError(t, err, "shouldn't be an error")
}

func TestParseRequiredErr(t *testing.T) {
	s := &strRequired{}
	err := Parse(s, nil)

	assert.NotNil(t, err, "should be an error")
}

func TestParseDefault(t *testing.T) {
	s := &strDefault{}
	SetDefaultValues(s)
	err := Parse(s, nil)

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, "sync", s.T, "wrong value")
}

func TestParseDuration(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	t.Run("with_default", func(t *testing.T) {
		t.Parallel()

		s := &struct {
			T  Duration `default:"5s" parse:"duration"`
			T_ time.Duration
		}{}
		SetDefaultValues(s)
		r.NoError(Parse(s, nil))
		r.Equal(time.Second*5, s.T_)
	})

	t.Run("without_default", func(t *testing.T) {
		t.Parallel()

		s := &struct {
			T  Duration `parse:"duration"`
			T_ time.Duration
		}{}
		SetDefaultValues(s)
		r.NoError(Parse(s, nil))
		r.Equal(time.Duration(0), s.T_)
	})
}

func TestParseOptionsOk(t *testing.T) {
	a := assert.New(t)

	s := &strOptions{T: "async"}
	a.NoError(Parse(s, nil))
	a.Equal(s.T_, PersistenceModeAsync)

	s.T = "sync"
	a.NoError(Parse(s, nil))
	a.Equal(s.T_, PersistenceModeSync)
}

func TestParseOptionsErr(t *testing.T) {
	s := &strOptions{T: "sequential"}
	err := Parse(s, nil)

	assert.NotNil(t, err, "should be an error")
	assert.Equal(t, PersistenceMode(0), s.T_)
}

func TestParseExpressionMul(t *testing.T) {
	s := &strExpression{T: "val*2"}
	err := Parse(s, map[string]int{"val": 3})

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, 6, s.T_, "wrong value")
}

func TestParseExpressionAdd(t *testing.T) {
	s := &strExpression{T: "10+val"}
	err := Parse(s, map[string]int{"val": 3})

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, 13, s.T_, "wrong value")
}

func TestParseExpressionConst(t *testing.T) {
	s := &strExpression{T: "10"}
	err := Parse(s, map[string]int{"val": 12})

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, 10, s.T_, "wrong value")
}

func TestParseDataUnitInvalid(t *testing.T) {
	TestList := []struct {
		strDataUnit   *strDataUnit
		ExpectedError error
		ExpectedValue uint
	}{
		{
			strDataUnit:   &strDataUnit{T: "10"},
			ExpectedError: errors.New("invalid data format, the string must contain 2 parts separated by a space"),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: " 10"},
			ExpectedError: errors.New(`can't parse uint: "" is not a number`),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: "10 "},
			ExpectedError: errors.New(`unexpected alias ""`),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: " 1 MB"},
			ExpectedError: errors.New("invalid data format, the string must contain 2 parts separated by a space"),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: " MB"},
			ExpectedError: errors.New(`can't parse uint: "" is not a number`),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: "MB "},
			ExpectedError: errors.New(`can't parse uint: "MB" is not a number`),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: "10 "},
			ExpectedError: errors.New(`unexpected alias ""`),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: "something"},
			ExpectedError: errors.New("invalid data format, the string must contain 2 parts separated by a space"),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: "some thing"},
			ExpectedError: errors.New(`can't parse uint: "some" is not a number`),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: ""},
			ExpectedError: errors.New("invalid data format, the string must contain 2 parts separated by a space"),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: "999999999999999999999 B"},
			ExpectedError: errors.New(`can't parse uint: "999999999999999999999" is not a number`),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: "1  B"},
			ExpectedError: errors.New("invalid data format, the string must contain 2 parts separated by a space"),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: "1 B "},
			ExpectedError: errors.New("invalid data format, the string must contain 2 parts separated by a space"),
			ExpectedValue: 0,
		},
		{
			strDataUnit:   &strDataUnit{T: "-1 PB"},
			ExpectedError: errors.New("value must be positive"),
			ExpectedValue: 0,
		},
		// TODO: handle uint overflow situation
		// {
		//	strDataUnit:       &strDataUnit{T: "100000000000 PB"},
		//	ExpectedError: errors.New("uint overflowed on product 100000000000 and PB"),
		//	ExpectedValue: 0,
		// },
	}
	for i := range TestList {
		err := Parse(TestList[i].strDataUnit, nil)
		assert.Equal(t, TestList[i].ExpectedError, err, "wrong error")
		assert.Equal(t, uint(0), TestList[i].strDataUnit.T_, "wrong value")
	}
}

func TestParseDataUnitValid(t *testing.T) {
	TestList := []struct {
		strDataUnit   *strDataUnit
		ExpectedValue uint
	}{
		{
			strDataUnit:   &strDataUnit{T: "10 MB"},
			ExpectedValue: 10000000,
		},
		{
			strDataUnit:   &strDataUnit{T: "10 mB"},
			ExpectedValue: 10000000,
		},
		{
			strDataUnit:   &strDataUnit{T: "10 Mb"},
			ExpectedValue: 10000000,
		},
		{
			strDataUnit:   &strDataUnit{T: "10 mb"},
			ExpectedValue: 10000000,
		},
		{
			strDataUnit:   &strDataUnit{T: "1 B"},
			ExpectedValue: 1,
		},
	}
	for i := range TestList {
		err := Parse(TestList[i].strDataUnit, nil)
		assert.Nil(t, err, "shouldn't be an error")
		assert.Equal(t, TestList[i].ExpectedValue, TestList[i].strDataUnit.T_, "wrong value")
	}
}

func TestParseFieldSelectorSimple(t *testing.T) {
	path := ParseFieldSelector("a.b.c")

	assert.Equal(t, 3, len(path), "wrong length")
	assert.Equal(t, "a", path[0], "wrong field")
	assert.Equal(t, "b", path[1], "wrong field")
	assert.Equal(t, "c", path[2], "wrong field")
}

func TestParseFieldSelectorEscape(t *testing.T) {
	path := ParseFieldSelector("a.b..c")

	assert.Equal(t, 2, len(path), "wrong length")
	assert.Equal(t, "a", path[0], "wrong field")
	assert.Equal(t, "b.c", path[1], "wrong field")
}

func TestParseFieldSelectorEnding(t *testing.T) {
	path := ParseFieldSelector("a.b.c..")

	assert.Equal(t, 3, len(path), "wrong length")
	assert.Equal(t, "a", path[0], "wrong field")
	assert.Equal(t, "b", path[1], "wrong field")
	assert.Equal(t, "c.", path[2], "wrong field")
}

func TestHierarchy(t *testing.T) {
	s := &hierarchy{T: "10"}
	err := Parse(s, map[string]int{})

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, "10", s.T, "wrong value")
	assert.Equal(t, "10", s.Child.T, "wrong value")
}

func TestSlice(t *testing.T) {
	s := &sliceStruct{Value: "parent_value", Childs: []sliceChild{{"child_1"}, {}}}
	SetDefaultValues(s)
	err := Parse(s, map[string]int{})

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, "parent_value", s.Value, "wrong value")
	assert.Equal(t, "child_1", s.Childs[0].Value, "wrong value")
	assert.Equal(t, "child", s.Childs[1].Value, "wrong value") // default value
}

func TestDefaultSlice(t *testing.T) {
	s := &sliceStruct{Value: "parent_value"}
	SetDefaultValues(s)
	err := Parse(s, map[string]int{})

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, "parent_value", s.Value, "wrong value")
	assert.NotEqual(t, nil, s.Childs, "wrong value")
	assert.Equal(t, 0, len(s.Childs), "wrong value")
}

func TestBase8Default(t *testing.T) {
	s := &strBase8{}
	SetDefaultValues(s)
	err := Parse(s, nil)
	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, int64(438), s.T_)
}

func TestBase8(t *testing.T) {
	s := &strBase8{T: "0777"}
	err := Parse(s, nil)
	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, int64(511), s.T_)
}

func TestApplyEnvs(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		environs [][]string
		wantJSON string
		wantErr  string
	}{
		{
			name: "should_ok_when_rewrite",
			json: `{"vault":{"address": "super_host"}}`,
			environs: [][]string{
				{"FILED_VAULT_ADDRESS", "http://127.0.0.1"},
				{"FILED_VAULT_TOKEN", "example_token"},
			},
			wantJSON: `{"vault": {"address": "http://127.0.0.1", "token": "example_token"}}`,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			object, err := simplejson.NewJson([]byte(tt.json))
			require.NoError(t, err)
			for _, env := range tt.environs {
				t.Setenv(env[0], env[1])
			}

			err = applyEnvs(object)

			if tt.wantErr == "" {
				require.NoError(t, err)
				got, errEnc := object.Encode()
				require.NoError(t, errEnc)
				require.JSONEq(t, tt.wantJSON, string(got))
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

type vaultMock struct {
	t                                   *testing.T
	secretPath, secretKey, secretResult string
	secretErr                           error
}

func newVaultMock(t *testing.T, path, key, result string, err error) *vaultMock {
	return &vaultMock{
		t:            t,
		secretPath:   path,
		secretKey:    key,
		secretResult: result,
		secretErr:    err,
	}
}

func (v *vaultMock) GetSecret(path, key string) (string, error) {
	assert.Equal(v.t, v.secretPath, path)
	assert.Equal(v.t, v.secretKey, key)
	return v.secretResult, v.secretErr
}

func (v *vaultMock) tryApply(s string) (string, bool) {
	return tryApplySecreter(v, s)
}

func TestApplyConfigFuncs(t *testing.T) {
	tests := []struct {
		name         string
		environs     [][]string
		json         string
		wantJSON     string
		wantErr      string
		secretKey    string
		secretPath   string
		secretResult string
		secretErr    error
	}{
		{
			name:         "should_ok_vault",
			json:         `{"welcome": {"input": {"type": "vault(test/test, value)"}}}`,
			secretPath:   "test/test",
			secretKey:    "value",
			secretResult: "result",
			wantJSON:     `{"welcome": {"input": {"type": "result"}}}`,
		},
		{
			name: "should_ok_env",
			environs: [][]string{
				{"ENV_1", "VAL_1"},
			},
			json:         `{"welcome": {"input": {"type": "env(ENV_1)"}}}`,
			secretPath:   "test/test",
			secretKey:    "value",
			secretResult: "result",
			wantJSON:     `{"welcome": {"input": {"type": "VAL_1"}}}`,
		},
		{
			name: "should_ok_env_and_vault",
			environs: [][]string{
				{"ENV_1", "VAL_1"},
			},
			json:         `{"welcome": {"input": {"type_1": "env(ENV_1)", "type_2": "vault(test/test, value)"}}}`,
			secretPath:   "test/test",
			secretKey:    "value",
			secretResult: "result",
			wantJSON:     `{"welcome": {"input": {"type_1": "VAL_1","type_2": "result"}}}`,
		},
		{
			name:         "should_ok_when_vault_in_array",
			json:         `{"welcome": {"input": {"type": ["vault(test/test, value)"]}}}`,
			secretPath:   "test/test",
			secretKey:    "value",
			secretResult: "result",
			wantJSON:     `{"welcome": {"input": {"type": ["result"]}}}`,
		},
		{
			name: "should_ok_when_env_in_array",
			environs: [][]string{
				{"ENV_1", "VAL_1"},
			},
			json:         `{"welcome": {"input": {"type": ["env(ENV_1)"]}}}`,
			secretPath:   "test/test",
			secretKey:    "value",
			secretResult: "result",
			wantJSON:     `{"welcome": {"input": {"type": ["VAL_1"]}}}`,
		},
		{
			name: "should_ok_when_env_and_vault_in_array",
			environs: [][]string{
				{"ENV_1", "VAL_1"},
			},
			json:         `{"welcome": {"input": {"type": ["env(ENV_1)","vault(test/test, value)"]}}}`,
			secretPath:   "test/test",
			secretKey:    "value",
			secretResult: "result",
			wantJSON:     `{"welcome": {"input": {"type": ["VAL_1","result"]}}}`,
		},
		{
			name:     "should_ok_when_vault_field_escaped",
			json:     `{"welcome": {"input": {"type": "\\vault(test/test, value)"}}}`,
			wantJSON: `{"welcome": {"input": {"type": "vault(test/test, value)"}}}`,
		},
		{
			name:     "should_ok_when_env_field_escaped",
			json:     `{"welcome": {"input": {"type": "\\env(ENV_1)"}}}`,
			wantJSON: `{"welcome": {"input": {"type": "env(ENV_1)"}}}`,
		},
		{
			name:     "should_ok_when_vault_syntax_invalid_no_bracket",
			json:     `{"welcome": {"input": {"type": "vault(test/test, value"}}}`,
			wantJSON: `{"welcome": {"input": {"type": "vault(test/test, value"}}}`,
		},
		{
			name:     "should_ok_when_env_syntax_invalid_no_bracket",
			json:     `{"welcome": {"input": {"type": "env(ENV_1"}}}`,
			wantJSON: `{"welcome": {"input": {"type": "env(ENV_1"}}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			object, err := simplejson.NewJson([]byte(tt.json))
			require.NoError(t, err)
			for _, env := range tt.environs {
				t.Setenv(env[0], env[1])
			}

			vault := newVaultMock(t, tt.secretPath, tt.secretKey, tt.secretResult, tt.secretErr)
			apps := []funcApplier{vault, &envs{}}

			applyConfigFuncs(apps, object)

			if tt.wantErr == "" {
				require.NoError(t, err)
				got, errEnc := object.Encode()
				require.NoError(t, errEnc)
				require.JSONEq(t, tt.wantJSON, string(got))
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestParseDefaultInt(t *testing.T) {
	testCases := []struct {
		s        *intDefault
		expected int
	}{
		{s: &intDefault{}, expected: 5},
		{s: &intDefault{T: 17}, expected: 17},
	}
	for i, tc := range testCases {
		SetDefaultValues(tc.s)
		err := Parse(tc.s, nil)

		assert.NoError(t, err, "shouldn't be an error tc: %d", i)
		assert.Equal(t, tc.expected, tc.s.T, "wrong value tc: %d", i)
	}
}

func TestParseDefaultBool(t *testing.T) {
	testCases := []struct {
		s        *boolDefault
		expected bool
	}{
		{s: &boolDefault{}, expected: true},
	}
	for i, tc := range testCases {
		SetDefaultValues(tc.s)
		err := Parse(tc.s, nil)

		assert.NoError(t, err, "shouldn't be an error tc: %d", i)
		assert.Equal(t, tc.expected, tc.s.T, "wrong value tc: %d", i)
	}
}

func TestPipelineValidatorValid(t *testing.T) {
	testName := []string{"pipeline_name", "PipeLine_NAME", "pipelinename", "PIPELINENAME", "pipeline_k8s"}
	for _, tl := range testName {
		assert.NoError(t, validatePipelineName(tl))
	}
}

func TestPipelineValidatorInvalid(t *testing.T) {
	testName := []string{"Pipeline-name", "pipeline-name", "<pipeline_name>", "пайплайн_нейм"}
	for _, tl := range testName {
		assert.Error(t, validatePipelineName(tl))
	}
}

func TestExpression_UnmarshalJSON(t *testing.T) {
	val := struct {
		E1 Expression `parse:"expression"`
		E2 Expression `parse:"expression"`
		E3 Expression `parse:"expression"`
	}{}

	require.NoError(t, json.Unmarshal([]byte(`{
		"E1": 1,
		"E2": "2",
		"E3": "2+2"
	}`), &val))

	require.Equal(t, Expression("1"), val.E1)
	require.Equal(t, Expression("2"), val.E2)
	require.Equal(t, Expression("2+2"), val.E3)
}
