package decoder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyslogParsePriority(t *testing.T) {
	tests := []struct {
		name string

		input string

		want       int
		wantOffset int
		wantErr    bool
	}{
		{
			name:       "valid_1",
			input:      "<1>",
			want:       1,
			wantOffset: 2,
		},
		{
			name:       "valid_2",
			input:      "<12>",
			want:       12,
			wantOffset: 3,
		},
		{
			name:       "valid_3",
			input:      "<123>",
			want:       123,
			wantOffset: 4,
		},
		{
			name:    "invalid_1",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid_2",
			input:   "<",
			wantErr: true,
		},
		{
			name:    "invalid_3",
			input:   "<>",
			wantErr: true,
		},
		{
			name:    "invalid_4",
			input:   "<str>",
			wantErr: true,
		},
		{
			name:    "invalid_5",
			input:   "<100000>",
			wantErr: true,
		},
		{
			name:    "invalid_6",
			input:   "<192>",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, offset, err := syslogParsePriority([]byte(tt.input))
			assert.Equal(t, tt.wantErr, err != nil)
			if tt.wantErr {
				return
			}

			assert.Equal(t, tt.want, p)
			assert.Equal(t, tt.wantOffset, offset)
		})
	}
}
