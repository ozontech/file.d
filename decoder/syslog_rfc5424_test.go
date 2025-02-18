package decoder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyslogRFC5424(t *testing.T) {
	tests := []struct {
		name string

		input  string
		params map[string]any

		want          SyslogRFC5424Row
		wantCreateErr bool
		wantDecodeErr bool
	}{
		{
			name:  "valid_full",
			input: "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"My \\\"Application\\\"\" eventID=\"1011\"] An application event log\n",
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority:  []byte("165"),
					Facility:  "20",
					Severity:  "5",
					Timestamp: []byte("2003-10-11T22:14:15.003Z"),
					Hostname:  []byte("mymachine.example.com"),
					AppName:   []byte("myproc"),
					ProcID:    []byte("10"),
					Message:   []byte("An application event log"),
				},
				ProtoVersion: []byte("1"),
				MsgID:        []byte("ID47"),
				StructuredData: SyslogSD{
					"exampleSDID@32473": SyslogSDParams{
						"iut":         []byte("3"),
						"eventSource": []byte("My \\\"Application\\\""),
						"eventID":     []byte("1011"),
					},
				},
			},
		},
		{
			name:  "valid_full_bom",
			input: fmt.Sprintf("<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] %sAn application event log", bom),
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority:  []byte("165"),
					Facility:  "20",
					Severity:  "5",
					Timestamp: []byte("2003-10-11T22:14:15.003Z"),
					Hostname:  []byte("mymachine.example.com"),
					AppName:   []byte("myproc"),
					ProcID:    []byte("10"),
					Message:   []byte("An application event log"),
				},
				ProtoVersion: []byte("1"),
				MsgID:        []byte("ID47"),
				StructuredData: SyslogSD{
					"exampleSDID@32473": SyslogSDParams{
						"iut":         []byte("3"),
						"eventSource": []byte("Application"),
						"eventID":     []byte("1011"),
					},
				},
			},
		},
		{
			name:  "valid_full_priority_format",
			input: "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] An application event log",
			params: map[string]any{
				syslogFacilityFormatParam: spfString,
				syslogSeverityFormatParam: spfString,
			},
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority:  []byte("165"),
					Facility:  "LOCAL4",
					Severity:  "NOTICE",
					Timestamp: []byte("2003-10-11T22:14:15.003Z"),
					Hostname:  []byte("mymachine.example.com"),
					AppName:   []byte("myproc"),
					ProcID:    []byte("10"),
					Message:   []byte("An application event log"),
				},
				ProtoVersion: []byte("1"),
				MsgID:        []byte("ID47"),
				StructuredData: SyslogSD{
					"exampleSDID@32473": SyslogSDParams{
						"iut":         []byte("3"),
						"eventSource": []byte("Application"),
						"eventID":     []byte("1011"),
					},
				},
			},
		},
		{
			name:  "valid_no_timestamp",
			input: "<165>1 - mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] An application event log",
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority: []byte("165"),
					Facility: "20",
					Severity: "5",
					Hostname: []byte("mymachine.example.com"),
					AppName:  []byte("myproc"),
					ProcID:   []byte("10"),
					Message:  []byte("An application event log"),
				},
				ProtoVersion: []byte("1"),
				MsgID:        []byte("ID47"),
				StructuredData: SyslogSD{
					"exampleSDID@32473": SyslogSDParams{
						"iut":         []byte("3"),
						"eventSource": []byte("Application"),
						"eventID":     []byte("1011"),
					},
				},
			},
		},
		{
			name:  "valid_no_hostname",
			input: "<165>1 2003-10-11T22:14:15.003Z - myproc 10 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] An application event log",
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority:  []byte("165"),
					Facility:  "20",
					Severity:  "5",
					Timestamp: []byte("2003-10-11T22:14:15.003Z"),
					AppName:   []byte("myproc"),
					ProcID:    []byte("10"),
					Message:   []byte("An application event log"),
				},
				ProtoVersion: []byte("1"),
				MsgID:        []byte("ID47"),
				StructuredData: SyslogSD{
					"exampleSDID@32473": SyslogSDParams{
						"iut":         []byte("3"),
						"eventSource": []byte("Application"),
						"eventID":     []byte("1011"),
					},
				},
			},
		},
		{
			name:  "valid_no_appname",
			input: "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com - 10 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] An application event log",
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority:  []byte("165"),
					Facility:  "20",
					Severity:  "5",
					Timestamp: []byte("2003-10-11T22:14:15.003Z"),
					Hostname:  []byte("mymachine.example.com"),
					ProcID:    []byte("10"),
					Message:   []byte("An application event log"),
				},
				ProtoVersion: []byte("1"),
				MsgID:        []byte("ID47"),
				StructuredData: SyslogSD{
					"exampleSDID@32473": SyslogSDParams{
						"iut":         []byte("3"),
						"eventSource": []byte("Application"),
						"eventID":     []byte("1011"),
					},
				},
			},
		},
		{
			name:  "valid_no_procid",
			input: "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc - ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] An application event log",
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority:  []byte("165"),
					Facility:  "20",
					Severity:  "5",
					Timestamp: []byte("2003-10-11T22:14:15.003Z"),
					Hostname:  []byte("mymachine.example.com"),
					AppName:   []byte("myproc"),
					Message:   []byte("An application event log"),
				},
				ProtoVersion: []byte("1"),
				MsgID:        []byte("ID47"),
				StructuredData: SyslogSD{
					"exampleSDID@32473": SyslogSDParams{
						"iut":         []byte("3"),
						"eventSource": []byte("Application"),
						"eventID":     []byte("1011"),
					},
				},
			},
		},
		{
			name:  "valid_no_msgid",
			input: "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 - [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] An application event log",
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority:  []byte("165"),
					Facility:  "20",
					Severity:  "5",
					Timestamp: []byte("2003-10-11T22:14:15.003Z"),
					Hostname:  []byte("mymachine.example.com"),
					AppName:   []byte("myproc"),
					ProcID:    []byte("10"),
					Message:   []byte("An application event log"),
				},
				ProtoVersion: []byte("1"),
				StructuredData: SyslogSD{
					"exampleSDID@32473": SyslogSDParams{
						"iut":         []byte("3"),
						"eventSource": []byte("Application"),
						"eventID":     []byte("1011"),
					},
				},
			},
		},
		{
			name:  "valid_no_sd",
			input: "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 - An application event log",
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority:  []byte("165"),
					Facility:  "20",
					Severity:  "5",
					Timestamp: []byte("2003-10-11T22:14:15.003Z"),
					Hostname:  []byte("mymachine.example.com"),
					AppName:   []byte("myproc"),
					ProcID:    []byte("10"),
					Message:   []byte("An application event log"),
				},
				ProtoVersion: []byte("1"),
				MsgID:        []byte("ID47"),
			},
		},
		{
			name:  "valid_no_msg",
			input: "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]",
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority:  []byte("165"),
					Facility:  "20",
					Severity:  "5",
					Timestamp: []byte("2003-10-11T22:14:15.003Z"),
					Hostname:  []byte("mymachine.example.com"),
					AppName:   []byte("myproc"),
					ProcID:    []byte("10"),
				},
				ProtoVersion: []byte("1"),
				MsgID:        []byte("ID47"),
				StructuredData: SyslogSD{
					"exampleSDID@32473": SyslogSDParams{
						"iut":         []byte("3"),
						"eventSource": []byte("Application"),
						"eventID":     []byte("1011"),
					},
				},
			},
		},
		{
			name:  "valid_only_required",
			input: "<165>1 - - - - - - An application event log",
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority: []byte("165"),
					Facility: "20",
					Severity: "5",
					Message:  []byte("An application event log"),
				},
				ProtoVersion: []byte("1"),
			},
		},
		{
			name:  "valid_multi_sd",
			input: "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [example1@123 param1=\"1\" param2=\"two\"][example2@123 param1=\"\" param2=\"twotwo\"] An application event log",
			want: SyslogRFC5424Row{
				SyslogRFC3164Row: SyslogRFC3164Row{
					Priority:  []byte("165"),
					Facility:  "20",
					Severity:  "5",
					Timestamp: []byte("2003-10-11T22:14:15.003Z"),
					Hostname:  []byte("mymachine.example.com"),
					AppName:   []byte("myproc"),
					ProcID:    []byte("10"),
					Message:   []byte("An application event log"),
				},
				ProtoVersion: []byte("1"),
				MsgID:        []byte("ID47"),
				StructuredData: SyslogSD{
					"example1@123": SyslogSDParams{
						"param1": []byte("1"),
						"param2": []byte("two"),
					},
					"example2@123": SyslogSDParams{
						"param1": []byte(""),
						"param2": []byte("twotwo"),
					},
				},
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
			name:          "invalid_decode_timestamp_1",
			input:         "<165>1 2003-10-11T22:14:15",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_2",
			input:         "<165>1 2003 10 11T22:14:15Z",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_3",
			input:         "<165>1 2003-10-11T22-14-15Z",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_4",
			input:         "<165>1 2003-13-11T22:14:15Z",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_5",
			input:         "<165>1 2003-12-32T22:14.15Z",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_6",
			input:         "<165>1 2003-12-31T25:14.15Z",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_7",
			input:         "<165>1 2003-12-31T22:62.15Z",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_8",
			input:         "<165>1 2003-12-31T22:14.99Z",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_9",
			input:         "<165>1 2003-12-31T22:14.15.0000003Z",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_10",
			input:         "<165>1 2003-12-31T22:14.15X",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_11",
			input:         "<165>1 2003-12-31T22:14.15-07",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_12",
			input:         "<165>1 2003-12-31T22:14.15@07:00",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_13",
			input:         "<165>1 2003-12-31T22:14.15+07@00",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_14",
			input:         "<165>1 2003-12-31T22:14.15+25:00",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_timestamp_15",
			input:         "<165>1 2003-12-31T22:14.15+07:65",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_hostname",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_appname",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_procid",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_msgid",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_sd_1",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 exampleSDID@32473",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_sd_2",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_sd_3",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473]",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_sd_4",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 ",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_sd_5",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 =]",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_sd_6",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut=3\"]",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_sd_7",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut=\"3]",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_sd_8",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut=\"3\" ",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_sd_9",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut=\"3\" ]",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_sd_10",
			input:         "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [ exampleSDID@32473 iut=\"3\"]",
			wantDecodeErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d, err := NewSyslogRFC5424Decoder(tt.params)
			assert.Equal(t, tt.wantCreateErr, err != nil)
			if tt.wantCreateErr {
				return
			}

			row, err := d.Decode([]byte(tt.input))
			assert.Equal(t, tt.wantDecodeErr, err != nil)
			if tt.wantDecodeErr {
				return
			}

			assert.Equal(t, tt.want, row.(SyslogRFC5424Row))
		})
	}
}
