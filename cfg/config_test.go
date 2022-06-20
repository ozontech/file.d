package cfg

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func NewTestConfig(name string) *Config {
	return NewConfigFromFile("../testdata/config/" + name)
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

type strDuration struct {
	T  Duration `default:"5s" parse:"duration"`
	T_ time.Duration
}

type strOptions struct {
	T string `default:"async" options:"async|sync"`
}

type strExpression struct {
	T  string `parse:"expression"`
	T_ int
}

type hierarchyChild struct {
	T string `required:"true"`
	U string
}

type hierarchy struct {
	T     string         `default:"sync"`
	Child hierarchyChild `child:"true"`
}

type hierarchyWithChildPtr struct {
	T     string          `default:"sync"`
	Child *hierarchyChild `child:"true"`
}

type sliceChild struct {
	Value string `default:"child"`
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

	assert.Error(t, err, "should be an error")
}

func TestParseDefault(t *testing.T) {
	s := &strDefault{}
	err := Parse(s, nil)

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, "sync", s.T, "wrong value")
}

func TestParseDuration(t *testing.T) {
	s := &strDuration{}
	err := Parse(s, nil)

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, time.Second*5, s.T_, "wrong value")
}

func TestParseOptionsOk(t *testing.T) {
	s := &strOptions{T: "async"}
	err := Parse(s, nil)

	assert.NoError(t, err, "shouldn't be an error")
}

func TestParseOptionsErr(t *testing.T) {
	s := &strOptions{T: "sequential"}
	err := Parse(s, nil)

	assert.Error(t, err, "should be an error")
}

func TestParseExpressionMul(t *testing.T) {
	s := &strExpression{T: "val*2"}
	err := Parse(s, map[string]int{"val": 3})

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, 6, s.T_, "wrong value")
}

func TestParseExpressionAdd(t *testing.T) {
	s := &strExpression{T: "10+val"}
	err := Parse(s, map[string]int{"val": 3})

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, 13, s.T_, "wrong value")
}

func TestParseExpressionConst(t *testing.T) {
	s := &strExpression{T: "10"}
	err := Parse(s, map[string]int{"val": 12})

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, 10, s.T_, "wrong value")
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

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, "10", s.T, "wrong value")
	assert.Equal(t, "10", s.Child.T, "wrong value")
}

func TestHierarchyChildPtr(t *testing.T) {
	t.Run("null_child", func(t *testing.T) {
		s := &hierarchyWithChildPtr{T: "10"}
		err := Parse(s, map[string]int{})

		assert.NoError(t, err, "shouldn't be an error")
		assert.Equal(t, "10", s.T, "wrong value")
		assert.Nil(t, s.Child, "nil prt child is null")
	})

	t.Run("ptr_child", func(t *testing.T) {
		s := &hierarchyWithChildPtr{
			T: "10",
			Child: &hierarchyChild{
				T: "1000",
				U: "1000",
			},
		}
		fmt.Println(s.T, s.Child.T)
		err := Parse(s, map[string]int{})

		assert.NoError(t, err, "shouldn't be an error")
		assert.Equal(t, "10", s.T, "wrong value")
		assert.Equal(t, "10", s.Child.T, "wrong value")
		assert.Equal(t, "1000", s.Child.U, "wrong value")
	})
}

func TestSlice(t *testing.T) {
	s := &sliceStruct{Value: "parent_value", Childs: []sliceChild{{"child_1"}, {}}}
	err := Parse(s, map[string]int{})

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, "parent_value", s.Value, "wrong value")
	assert.Equal(t, "child_1", s.Childs[0].Value, "wrong value")
	assert.Equal(t, "child", s.Childs[1].Value, "wrong value") // default value
}

func TestDefaultSlice(t *testing.T) {
	s := &sliceStruct{Value: "parent_value"}
	err := Parse(s, map[string]int{})

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, "parent_value", s.Value, "wrong value")
	assert.NotEqual(t, nil, s.Childs, "wrong value")
	assert.Equal(t, 0, len(s.Childs), "wrong value")
}

func TestBase8Default(t *testing.T) {
	s := &strBase8{}
	err := Parse(s, nil)
	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, int64(438), s.T_)
}

func TestBase8(t *testing.T) {
	s := &strBase8{T: "0777"}
	err := Parse(s, nil)
	assert.NoError(t, err, "shouldn't be an error")
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
			json, err := simplejson.NewJson([]byte(tt.json))
			require.NoError(t, err)
			for _, env := range tt.environs {
				errS := os.Setenv(env[0], env[1])
				require.NoError(t, errS)
				defer func(key string) {
					errU := os.Unsetenv(key)
					require.NoError(t, errU)
				}(env[0])
			}

			err = applyEnvs(json)

			if tt.wantErr == "" {
				require.NoError(t, err)
				got, errEnc := json.Encode()
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

func TestApplyVault(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		wantJSON string
		wantErr  string

		secretKey, secretPath, secretResult string
		secretErr                           error
	}{
		{
			name:         "should_ok",
			json:         `{"welcome": {"input": {"type": "vault(test/test, value)"}}}`,
			secretPath:   "test/test",
			secretKey:    "value",
			secretResult: "result",
			wantJSON:     `{"welcome": {"input": {"type": "result"}}}`,
		},
		{
			name:         "should_ok_when_value_in_array",
			json:         `{"welcome": {"input": {"type": ["vault(test/test, value)"]}}}`,
			secretPath:   "test/test",
			secretKey:    "value",
			secretResult: "result",
			wantJSON:     `{"welcome": {"input": {"type": ["result"]}}}`,
		},
		{
			name:     "should_ok_when_vault_field_escaped",
			json:     `{"welcome": {"input": {"type": "\\vault(test/test, value)"}}}`,
			wantJSON: `{"welcome": {"input": {"type": "vault(test/test, value)"}}}`,
		},
		{
			name:     "should_ok_when_vault_syntax_invalid_no_bracket",
			json:     `{"welcome": {"input": {"type": "vault(test/test, value"}}}`,
			wantJSON: `{"welcome": {"input": {"type": "vault(test/test, value"}}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			json, err := simplejson.NewJson([]byte(tt.json))
			require.NoError(t, err)
			vault := newVaultMock(t, tt.secretPath, tt.secretKey, tt.secretResult, tt.secretErr)

			applyVault(vault, json)

			if tt.wantErr == "" {
				require.NoError(t, err)
				got, errEnc := json.Encode()
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
		err := Parse(tc.s, nil)

		assert.NoError(t, err, "shouldn't be an error tc: %d", i)
		assert.Equal(t, tc.expected, tc.s.T, "wrong value tc: %d", i)
	}
}
