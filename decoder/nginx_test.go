package decoder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNginxError(t *testing.T) {
	tests := []struct {
		name string

		input   string
		want    NginxErrorRow
		wantErr bool
	}{
		{
			name:  "valid_full",
			input: `2022/08/18 09:29:37 [error] 844935#844935: *44934601 upstream timed out (110: Operation timed out) while connecting to upstream, client: 10.125.172.251, server: mpm-youtube-downloader-38.name.tldn, request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84`,
			want: NginxErrorRow{
				Time:    []byte("2022/08/18 09:29:37"),
				Level:   []byte("error"),
				PID:     []byte("844935"),
				TID:     []byte("844935"),
				CID:     []byte("44934601"),
				Message: []byte(`upstream timed out (110: Operation timed out) while connecting to upstream, client: 10.125.172.251, server: mpm-youtube-downloader-38.name.tldn, request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84`),
			},
		},
		{
			name:  "valid_no_cid",
			input: `2022/08/18 09:29:37 [error] 844935#844935: upstream timed out (110: Operation timed out) while connecting to upstream, client: 10.125.172.251, server: mpm-youtube-downloader-38.name.tldn, request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84`,
			want: NginxErrorRow{
				Time:    []byte("2022/08/18 09:29:37"),
				Level:   []byte("error"),
				PID:     []byte("844935"),
				TID:     []byte("844935"),
				Message: []byte(`upstream timed out (110: Operation timed out) while connecting to upstream, client: 10.125.172.251, server: mpm-youtube-downloader-38.name.tldn, request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84`),
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
			name:    "invalid_1",
			input:   " ",
			wantErr: true,
		},
		{
			name:    "invalid_2",
			input:   "invalid",
			wantErr: true,
		},
		{
			name:    "invalid_3",
			input:   "2022/08/18 09:38:25",
			wantErr: true,
		},
		{
			name:    "invalid_4",
			input:   "2022/08/18 09:38:25 message",
			wantErr: true,
		},
		{
			name:    "invalid_5",
			input:   "2022/08/18 09:38:25 [] message",
			wantErr: true,
		},
		{
			name:    "invalid_6",
			input:   "2022/08/18 09:38:25 [error] ",
			wantErr: true,
		},
		{
			name:    "invalid_7",
			input:   "2022/08/18 09:38:25 [error] pid_tid: ",
			wantErr: true,
		},
		{
			name:    "invalid_8",
			input:   "2022/08/18 09:38:25 [error] pid#tid ",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			row, err := DecodeNginxError([]byte(tt.input))
			assert.Equal(t, tt.wantErr, err != nil)
			if tt.wantErr {
				return
			}

			assert.Equal(t, tt.want, row)
		})
	}

}
