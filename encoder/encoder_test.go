package encoder

import (
	"encoding/json"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEvent(t testing.TB, raw string) *pipeline.Event {
	t.Helper()

	root, err := insaneJSON.DecodeString(raw)
	require.NoError(t, err)

	t.Cleanup(func() {
		insaneJSON.Release(root)
	})

	return &pipeline.Event{Root: root}
}

func TestNewEncoder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         EncodingConfig
		wantErr     bool
		errContains string
		assertType  func(t *testing.T, enc Encoder)
	}{
		{
			name: "explicit json type",
			cfg:  EncodingConfig{Type: EncoderTypeJSON},
			assertType: func(t *testing.T, enc Encoder) {
				assert.IsType(t, &JSONEncoder{}, enc)
			},
		},
		{
			name: "empty type defaults to json",
			cfg:  EncodingConfig{Type: ""},
			assertType: func(t *testing.T, enc Encoder) {
				assert.IsType(t, &JSONEncoder{}, enc)
			},
		},
		{
			name: "raw type without params uses default field",
			cfg:  EncodingConfig{Type: EncoderTypeRaw},
			assertType: func(t *testing.T, enc Encoder) {
				raw, ok := enc.(*RawEncoder)
				require.True(t, ok)
				assert.Equal(t, "message", raw.field)
			},
		},
		{
			name: "raw type with empty params object uses default field",
			cfg:  EncodingConfig{Type: EncoderTypeRaw, Params: json.RawMessage(`{}`)},
			assertType: func(t *testing.T, enc Encoder) {
				raw, ok := enc.(*RawEncoder)
				require.True(t, ok)
				assert.Equal(t, "message", raw.field)
			},
		},
		{
			name: "raw type with custom field",
			cfg:  EncodingConfig{Type: EncoderTypeRaw, Params: json.RawMessage(`{"field":"data"}`)},
			assertType: func(t *testing.T, enc Encoder) {
				raw, ok := enc.(*RawEncoder)
				require.True(t, ok)
				assert.Equal(t, "data", raw.field)
			},
		},
		{
			name: "raw type with empty field falls back to message",
			cfg:  EncodingConfig{Type: EncoderTypeRaw, Params: json.RawMessage(`{"field":""}`)},
			assertType: func(t *testing.T, enc Encoder) {
				raw, ok := enc.(*RawEncoder)
				require.True(t, ok)
				assert.Equal(t, "message", raw.field)
			},
		},
		{
			name:        "raw type with invalid params",
			cfg:         EncodingConfig{Type: EncoderTypeRaw, Params: json.RawMessage(`{"field":`)},
			wantErr:     true,
			errContains: "raw encoder params",
		},
		{
			name:        "unknown type",
			cfg:         EncodingConfig{Type: "yaml"},
			wantErr:     true,
			errContains: `unknown encoding type "yaml"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			enc, err := NewEncoder(tt.cfg)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, enc)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, enc)
			if tt.assertType != nil {
				tt.assertType(t, enc)
			}
		})
	}
}

func TestEncode(t *testing.T) {
	t.Parallel()

	t.Run("json", func(t *testing.T) {
		t.Parallel()

		enc, err := NewEncoder(EncodingConfig{Type: EncoderTypeJSON})
		require.NoError(t, err)

		event := newTestEvent(t, `{"message":"hi"}`)
		out, err := enc.Encode(event, nil)
		require.NoError(t, err)
		assert.JSONEq(t, `{"message":"hi"}`, string(out))
	})

	t.Run("raw", func(t *testing.T) {
		t.Parallel()

		enc, err := NewEncoder(EncodingConfig{
			Type:   EncoderTypeRaw,
			Params: json.RawMessage(`{"field":"message"}`),
		})
		require.NoError(t, err)

		event := newTestEvent(t, `{"message":"hi"}`)
		out, err := enc.Encode(event, nil)
		require.NoError(t, err)
		assert.Equal(t, "hi", string(out))
	})
}
