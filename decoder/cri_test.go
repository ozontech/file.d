package decoder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCRIPartial(t *testing.T) {
	row, err := DecodeCRI([]byte("2016-10-06T00:17:09.669794202Z stdout P partial content 1\n"))

	assert.NoError(t, err, "error while decoding cri log")
	assert.Equal(t, "2016-10-06T00:17:09.669794202Z", string(row.Time))
	assert.Equal(t, "stdout", string(row.Stream))
	assert.Equal(t, "partial content 1", string(row.Log))
	assert.Equal(t, true, row.IsPartial)
}

func TestCRIFull(t *testing.T) {
	row, err := DecodeCRI([]byte("2016-10-06T00:17:09.669794202Z stdout F full content 2\n"))

	assert.NoError(t, err, "error while decoding cri log")
	assert.Equal(t, "2016-10-06T00:17:09.669794202Z", string(row.Time))
	assert.Equal(t, "stdout", string(row.Stream))
	assert.Equal(t, "full content 2\n", string(row.Log))
	assert.Equal(t, false, row.IsPartial)
}

func TestCRIError(t *testing.T) {
	_, err := DecodeCRI([]byte("2016-10-06T00:17:09.669794202Z stdout  full content 3\n"))

	assert.Error(t, err, "there must be an error")
}

func TestCRIErrorJoined2Lines(t *testing.T) {
	_, err := DecodeCRI([]byte("2016-10-06T00:17:09.669794202Z s2024-05-22T10:15:04.129321194Z 3\n"))

	assert.Error(t, err, "there must be an error")
}

func TestCRIJoined2Lines(t *testing.T) {
	row, err := DecodeCRI([]byte("2024-05-22T09:51:04.025764351Z s2024-05-22T10:15:04.129321194Z stderr F 2024/05/22 10:15:04 start prepraring file\n"))

	assert.NoError(t, err, "error while decoding cri log")
	assert.Equal(t, "2024-05-22T09:51:04.025764351Z", string(row.Time))
	assert.Equal(t, "stderr", string(row.Stream))
	assert.Equal(t, "2024/05/22 10:15:04 start prepraring file\n", string(row.Log))
	assert.Equal(t, false, row.IsPartial)
}

func TestCRIJoined3Lines(t *testing.T) {
	row, err := DecodeCRI([]byte(`2024-05-22T09:51:04.025764351Z s2024-05-22T10:15:04.129321194Z stder2024-05-22T01:16:15.512917014Z stdo 2024-05-22T06:39:29.230708414Z stdout F {"level":"warn","ts":"2024-05-22T06:39:29.230Z"}\n`))

	assert.NoError(t, err, "error while decoding cri log")
	assert.Equal(t, "2024-05-22T09:51:04.025764351Z", string(row.Time))
	assert.Equal(t, "stdout", string(row.Stream))
	assert.Equal(t, `{"level":"warn","ts":"2024-05-22T06:39:29.230Z"}\n`, string(row.Log))
	assert.Equal(t, false, row.IsPartial)
}
