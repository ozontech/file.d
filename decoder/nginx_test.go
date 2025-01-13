package decoder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNginxError(t *testing.T) {
	tests := []struct {
		name string

		input  string
		params map[string]any

		want          NginxErrorRow
		wantCreateErr bool
		wantDecodeErr bool
	}{
		{
			name:  "valid_full",
			input: `2022/08/18 09:29:37 [error] 844935#844935: *44934601 upstream timed out (110: Operation timed out) while connecting to upstream, client: 10.125.172.251, server: mpm-youtube-downloader-38.name.tldn, request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84"` + "\n",
			want: NginxErrorRow{
				Time:    []byte("2022/08/18 09:29:37"),
				Level:   []byte("error"),
				PID:     []byte("844935"),
				TID:     []byte("844935"),
				CID:     []byte("44934601"),
				Message: []byte(`upstream timed out (110: Operation timed out) while connecting to upstream, client: 10.125.172.251, server: mpm-youtube-downloader-38.name.tldn, request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84"`),
			},
		},
		{
			name:  "valid_no_cid",
			input: `2022/08/18 09:29:37 [error] 844935#844935: upstream timed out (110: Operation timed out) while connecting to upstream, client: 10.125.172.251, server: mpm-youtube-downloader-38.name.tldn, request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84"`,
			want: NginxErrorRow{
				Time:    []byte("2022/08/18 09:29:37"),
				Level:   []byte("error"),
				PID:     []byte("844935"),
				TID:     []byte("844935"),
				Message: []byte(`upstream timed out (110: Operation timed out) while connecting to upstream, client: 10.125.172.251, server: mpm-youtube-downloader-38.name.tldn, request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84"`),
			},
		},
		{
			name:  "valid_no_message",
			input: `2022/08/18 09:29:37 [error] 844935#844935: *44934601 `,
			want: NginxErrorRow{
				Time:  []byte("2022/08/18 09:29:37"),
				Level: []byte("error"),
				PID:   []byte("844935"),
				TID:   []byte("844935"),
				CID:   []byte("44934601"),
			},
		},
		{
			name:  "valid_no_cid_no_message",
			input: `2022/08/18 09:29:37 [error] 844935#844935: `,
			want: NginxErrorRow{
				Time:  []byte("2022/08/18 09:29:37"),
				Level: []byte("error"),
				PID:   []byte("844935"),
				TID:   []byte("844935"),
			},
		},
		{
			name:  "valid_custom_fields",
			input: `2022/08/18 09:29:37 [error] 844935#844935: *44934601 upstream timed out (110: Operation timed out), while connecting to upstream, client: 10.125.172.251, server: , request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84"` + "\n",
			params: map[string]any{
				nginxWithCustomFieldsParam: true,
			},
			want: NginxErrorRow{
				Time:    []byte("2022/08/18 09:29:37"),
				Level:   []byte("error"),
				PID:     []byte("844935"),
				TID:     []byte("844935"),
				CID:     []byte("44934601"),
				Message: []byte(`upstream timed out (110: Operation timed out), while connecting to upstream`),
				CustomFields: map[string][]byte{
					"client":   []byte("10.125.172.251"),
					"server":   []byte(""),
					"request":  []byte("POST /download HTTP/1.1"),
					"upstream": []byte("http://10.117.246.15:84/download"),
					"host":     []byte("mpm-youtube-downloader-38.name.tldn:84"),
				},
			},
		},
		{
			name: "invalid_create",
			params: map[string]any{
				nginxWithCustomFieldsParam: "not bool",
			},
			wantCreateErr: true,
		},
		{
			name:          "invalid_decode_1",
			input:         " ",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_2",
			input:         "invalid",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_3",
			input:         "2022/08/18 09:38:25",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_4",
			input:         "2022/08/18 09:38:25 message",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_5",
			input:         "2022/08/18 09:38:25 [] message",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_6",
			input:         "2022/08/18 09:38:25 [error] ",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_7",
			input:         "2022/08/18 09:38:25 [error] pid_tid: ",
			wantDecodeErr: true,
		},
		{
			name:          "invalid_decode_8",
			input:         "2022/08/18 09:38:25 [error] pid#tid ",
			wantDecodeErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d, err := NewNginxErrorDecoder(tt.params)
			assert.Equal(t, tt.wantCreateErr, err != nil)
			if tt.wantCreateErr {
				return
			}

			row, err := d.Decode([]byte(tt.input))
			assert.Equal(t, tt.wantDecodeErr, err != nil)
			if tt.wantDecodeErr {
				return
			}

			assert.Equal(t, tt.want, row.(NginxErrorRow))
		})
	}
}
