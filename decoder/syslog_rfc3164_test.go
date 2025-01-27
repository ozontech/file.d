package decoder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyslogRFC3164(t *testing.T) {
	tests := []struct {
		name string

		input  string
		params map[string]any

		want          SyslogRFC3164Row
		wantCreateErr bool
		wantDecodeErr bool
	}{
		{
			name:  "valid_full",
			input: "<34>Oct 11 22:14:15 mymachine.example.com myproc[10]: 'myproc' failed on /dev/pts/8\n",
			want: SyslogRFC3164Row{
				Priority:  []byte("34"),
				Facility:  "4",
				Severity:  "2",
				Timestamp: []byte("Oct 11 22:14:15"),
				Hostname:  []byte("mymachine.example.com"),
				AppName:   []byte("myproc"),
				PID:       []byte("10"),
				Message:   []byte("'myproc' failed on /dev/pts/8"),
			},
		},
		{
			name:  "valid_no_pid",
			input: "<4>Oct  5 22:14:15 mymachine.example.com myproc: 'myproc' failed on /dev/pts/8",
			want: SyslogRFC3164Row{
				Priority:  []byte("4"),
				Facility:  "0",
				Severity:  "4",
				Timestamp: []byte("Oct  5 22:14:15"),
				Hostname:  []byte("mymachine.example.com"),
				AppName:   []byte("myproc"),
				Message:   []byte("'myproc' failed on /dev/pts/8"),
			},
		},
		{
			name:  "valid_priority_format",
			input: "<34>Oct 11 22:14:15 mymachine.example.com myproc[10]: 'myproc' failed on /dev/pts/8\n",
			params: map[string]any{
				syslogFacilityFormatParam: spfString,
				syslogSeverityFormatParam: spfString,
			},
			want: SyslogRFC3164Row{
				Priority:  []byte("34"),
				Facility:  "AUTH",
				Severity:  "CRIT",
				Timestamp: []byte("Oct 11 22:14:15"),
				Hostname:  []byte("mymachine.example.com"),
				AppName:   []byte("myproc"),
				PID:       []byte("10"),
				Message:   []byte("'myproc' failed on /dev/pts/8"),
			},
		},
		{
			name: "invalid_create_1",
			params: map[string]any{
				syslogFacilityFormatParam: spfString,
				syslogSeverityFormatParam: 123,
			},
			wantCreateErr: true,
		},
		{
			name: "invalid_create_2",
			params: map[string]any{
				syslogFacilityFormatParam: "test",
			},
			wantCreateErr: true,
		},
		{
			name:          "invalid_decode_pri_1",
			input:         "<>",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_pri_2",
			input:         "<str>",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_pri_3",
			input:         "<100000>",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_pri_4",
			input:         "<192>",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_1",
			input:         "<34> Oct 11 22:14:15",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_2",
			input:         "<34>2006-01-02 15:04:05",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_3",
			input:         "<34>Oct 2  22:14:15 ",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_4",
			input:         "<34>Oct 11 22:14:15test",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_5",
			input:         "<34>Oct 11 aa:bb:cc ",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_6",
			input:         "<34>oct 11 22:14:15 ",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_hostname",
			input:         "<34>Oct 11 22:14:15 mymachine.example.com",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_appname",
			input:         "<34>Oct 11 22:14:15 mymachine.example.com myproc",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_pid_1",
			input:         "<34>Oct 11 22:14:15 mymachine.example.com myproc[10: 'myproc' failed on /dev/pts/8",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_pid_2",
			input:         "<34>Oct 11 22:14:15 mymachine.example.com myproc[10] 'myproc' failed on /dev/pts/8",
			wantDecodeErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d, err := NewSyslogRFC3164Decoder(tt.params)
			assert.Equal(t, tt.wantCreateErr, err != nil)
			if tt.wantCreateErr {
				return
			}

			row, err := d.Decode([]byte(tt.input))
			assert.Equal(t, tt.wantDecodeErr, err != nil)
			if tt.wantDecodeErr {
				return
			}

			assert.Equal(t, tt.want, row.(SyslogRFC3164Row))
		})
	}
}
