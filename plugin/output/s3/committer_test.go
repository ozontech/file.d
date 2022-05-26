package s3

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/mocks/pipeline_plugin_mock"
	"github.com/ozontech/file.d/plugin/output/kafka"
)

func TestNewCommiterFabric(t *testing.T) {
	cases := []struct {
		name     string
		config   *Config
		params   *pipeline.OutputPluginParams
		expected *KafkaCommitterFabric
	}{
		{
			name:     "no_config",
			config:   &Config{},
			params:   nil,
			expected: nil,
		},
		{
			name: "kafka_config",
			config: &Config{
				CommitCfg: &commitConfig{
					CommitterType: "kafka",
					KafkaCfg:      kafka.Config{Brokers: []string{"b1", "b2", "b3"}},
				},
			},
			params: &pipeline.OutputPluginParams{},
			expected: &KafkaCommitterFabric{
				config: kafka.Config{Brokers: []string{"b1", "b2", "b3"}},
				params: &pipeline.OutputPluginParams{},
			},
		},
	}
	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			commiterFabric, err := NewCommitterFabric(tCase.config, tCase.params)
			require.NoError(t, err)
			require.Equal(t, tCase.expected, commiterFabric)
		})
	}
}

func TestNewCommiterFabricError(t *testing.T) {
	commiterFabric, err := NewCommitterFabric(&Config{
		CommitCfg: &commitConfig{
			CommitterType: "bad type",
		}},
		nil,
	)
	require.Error(t, err)
	require.EqualError(t, errors.New("unknown commiter type: bad type"), err.Error())
	require.Nil(t, commiterFabric)
}

func TestCommiterWrapperMultiVals(t *testing.T) {
	constantPart := `"foo":"bar"`
	values := map[string]interface{}{
		"string_key":    "sstr",
		"string_key2":   "sstr2",
		"int_key":       int(1),
		"int_key_2":     int(-1),
		"int8_key":      int8(2),
		"int8_key_2":    int8(-2),
		"int16_key":     int16(3),
		"int16_key_2":   int16(-3),
		"int32_key":     int32(4),
		"int32_key_2":   int32(-4),
		"int64_key":     int64(5),
		"int64_key_2":   int64(-5),
		"uint_key":      uint(10),
		"uint_key_2":    uint(11),
		"uint8_key":     uint8(20),
		"uint8_key_2":   uint8(21),
		"uint16_key":    uint16(30),
		"uint16_key_2":  uint16(31),
		"uint32_key":    uint32(40),
		"uint32_key_2":  uint32(41),
		"uint64_key":    uint64(42),
		"uint64_key_2":  uint64(42),
		"float32_key":   float32(3.2),
		"float32_key_2": float32(3.3),
		"float64_key":   float64(6.4),
		"float64_key_2": float64(6.5),
		"nil_key":       nil,
		"nil_key_2":     nil}

	expEncodedStringNoBrackets := `"string_key":"sstr","string_key2":"sstr2","float64_key":6.4,"float32_key":3.200000047683716,"float32_key_2":3.299999952316284,"float64_key_2":6.5,"foo":"bar","int8_key":2,"int16_key":3,"int32_key":4,"int64_key":5,"int64_key_2":-5,"int_key_2":-1,"int8_key_2":-2,"uint_key_2":11,"uint64_key_2":42,"nil_key":null,"nil_key_2":null,"int16_key_2":-3,"uint8_key":20,"uint8_key_2":21,"uint64_key":42,"int_key":1,"int32_key_2":-4,"uint_key":10,"uint16_key":30,"uint16_key_2":31,"uint32_key":40,"uint32_key_2":41`

	var err error
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mockPlugin := pipeline_plugin_mock.NewMockOutputPlugin(ctl)

	root := insaneJSON.Spawn()
	err = root.DecodeString(fmt.Sprintf("{%s}", constantPart))
	require.NoError(t, err)

	event := &pipeline.Event{Root: root}
	event.SetCustomCommitKind()
	defer insaneJSON.Release(root)

	mockPlugin.
		EXPECT().
		// Root ptrs differs for events, therefore TypeOf and Do required to validate event.
		Out(gomock.AssignableToTypeOf(&pipeline.Event{})).
		Do(func(ev *pipeline.Event) {
			splited := strings.Split(ev.Root.EncodeToString(), ",")
			// cut {} brackets.
			splited[0] = splited[0][1:]
			splited[len(splited)-1] = splited[len(splited)-1][:len(splited[len(splited)-1])-1]

			require.ElementsMatch(t, strings.Split(expEncodedStringNoBrackets, ","), splited)
		}).
		Return()

	wrapper := NewCommiterWrapper(mockPlugin)

	err = wrapper.CommitUpload(values, constantPart)
	require.NoError(t, err)
}

func TestCommitterWrapper(t *testing.T) {
	cases := []struct {
		name             string
		constantPart     string
		values           map[string]interface{}
		expEncodedString string
	}{
		{
			name:             "only_constant_part",
			constantPart:     `"foo":"bar", "get":"add"`,
			values:           nil,
			expEncodedString: `{"foo":"bar","get":"add"}`,
		},
		{
			name:             "cast_string",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"string_key": "sstr"},
			expEncodedString: `{"foo":"bar","string_key":"sstr"}`,
		},
		{
			name:             "cast_int",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"int_key": int(1)},
			expEncodedString: `{"foo":"bar","int_key":1}`,
		},
		{
			name:             "cast_int8",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"int8_key": int8(2)},
			expEncodedString: `{"foo":"bar","int8_key":2}`,
		},
		{
			name:             "cast_int16",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"int16_key": int16(3)},
			expEncodedString: `{"foo":"bar","int16_key":3}`,
		},
		{
			name:             "cast_int32",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"int32_key": int32(-4)},
			expEncodedString: `{"foo":"bar","int32_key":-4}`,
		},
		{
			name:             "cast_int64",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"int64_key": int64(-5)},
			expEncodedString: `{"foo":"bar","int64_key":-5}`,
		},
		{
			name:             "cast_uint",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"uint_key": uint(10)},
			expEncodedString: `{"foo":"bar","uint_key":10}`,
		},
		{
			name:             "cast_uint8",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"uint8_key": uint8(11)},
			expEncodedString: `{"foo":"bar","uint8_key":11}`,
		},
		{
			name:             "cast_uint16",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"uint16_key": uint16(12)},
			expEncodedString: `{"foo":"bar","uint16_key":12}`,
		},
		{
			name:             "cast_uint32",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"uint32_key": uint32(13)},
			expEncodedString: `{"foo":"bar","uint32_key":13}`,
		},
		{
			name:             "cast_uint64",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"uint64_key": uint64(14)},
			expEncodedString: `{"foo":"bar","uint64_key":14}`,
		},
		{
			name:             "cast_bool",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"bool_key": true},
			expEncodedString: `{"foo":"bar","bool_key":true}`,
		},
		{
			name:             "cast_float32",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"float32_key": 1.11},
			expEncodedString: `{"foo":"bar","float32_key":1.11}`,
		},
		{
			name:             "cast_float64",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"float64_key": 2.22},
			expEncodedString: `{"foo":"bar","float64_key":2.22}`,
		},
		{
			name:             "cast_nil",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"nil_key": nil},
			expEncodedString: `{"foo":"bar","nil_key":null}`,
		},
		{
			name:             "skip_invalid_elem",
			constantPart:     `"foo":"bar"`,
			values:           map[string]interface{}{"slice": []int{1, 2, 3}},
			expEncodedString: `{"foo":"bar"}`,
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {

			var err error
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockPlugin := pipeline_plugin_mock.NewMockOutputPlugin(ctl)

			root := insaneJSON.Spawn()
			err = root.DecodeString(fmt.Sprintf("{%s}", tCase.constantPart))
			require.NoError(t, err)

			event := &pipeline.Event{Root: root}
			event.SetCustomCommitKind()
			defer insaneJSON.Release(root)

			mockPlugin.
				EXPECT().
				// Root ptrs differs for events, therefore TypeOf and Do required to validate event.
				Out(gomock.AssignableToTypeOf(&pipeline.Event{})).
				Do(func(ev *pipeline.Event) {
					require.Equal(t, tCase.expEncodedString, ev.Root.EncodeToString())
				}).
				Return()

			wrapper := NewCommiterWrapper(mockPlugin)

			err = wrapper.CommitUpload(tCase.values, tCase.constantPart)
			require.NoError(t, err)
		})
	}
}

func TestCommiterWrapperError(t *testing.T) {
	badConstantPart := `bad_json`
	values := map[string]interface{}{}

	var err error
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	mockPlugin := pipeline_plugin_mock.NewMockOutputPlugin(ctl)
	root := insaneJSON.Spawn()
	event := &pipeline.Event{Root: root}
	event.SetCustomCommitKind()
	defer insaneJSON.Release(root)

	wrapper := NewCommiterWrapper(mockPlugin)

	err = wrapper.CommitUpload(values, badConstantPart)
	require.Error(t, err)
	require.EqualError(t, errors.New("expected object field near `{bad_json`\n                             ^"), err.Error())
}

func TestFabricCommiterFunc(t *testing.T) {
	kafkaFabric, err := NewCommitterFabric(&Config{CommitCfg: &commitConfig{CommitterType: "kafka"}}, nil)
	require.NoError(t, err)
	require.NotNil(t, kafkaFabric)

	starter := kafkaFabric.CommitterFunc()

	var starterType CommiterStarter
	require.NotNil(t, starter)
	require.IsType(t, starterType, starter)
}

func TestFabricIsNotNil(t *testing.T) {
	kafkaFabric, err := NewCommitterFabric(&Config{CommitCfg: &commitConfig{CommitterType: "kafka"}}, nil)
	require.NoError(t, err)
	require.NotNil(t, kafkaFabric)
}
