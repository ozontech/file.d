package decoder

import (
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
)

func TestPostgres(t *testing.T) {
	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	err := DecodePostgresToJson(root, []byte("2021-06-22 16:24:27 GMT [7291] => [3-1] client=test_client,db=test_db,user=test_user LOG:  listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\"\n"))

	assert.NoError(t, err, "error while decoding postgres log")
	assert.Equal(t, "2021-06-22 16:24:27 GMT", root.Dig("time").AsString())
	assert.Equal(t, "7291", root.Dig("pid").AsString())
	assert.Equal(t, "3-1", root.Dig("pid_message_number").AsString())
	assert.Equal(t, "test_client", root.Dig("client").AsString())
	assert.Equal(t, "test_db", root.Dig("db").AsString())
	assert.Equal(t, "test_user", root.Dig("user").AsString())
	assert.Equal(t, "listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\"\n", root.Dig("log").AsString())
}
