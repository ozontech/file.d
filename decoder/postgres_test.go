package decoder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	insaneJSON "github.com/vitkovskii/insane-json"
)

func TestPostgres(t *testing.T) {
	root := insaneJSON.Spawn()
	err := DecodePostgres(root, []byte("2021-06-22 16:24:27 GMT [7291] => [3-1] client=test_client,db=test_db,user=test_user LOG:  listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\"\n"))

	assert.NoError(t, err, "error while decoding cri log")
	assert.Equal(t, "2021-06-22 16:24:27 GMT", root.Dig("time").AsString())
	assert.Equal(t, "7291", root.Dig("pid").AsString())
	assert.Equal(t, "3-1", root.Dig("pid_message_number").AsString())
	assert.Equal(t, "test_client", root.Dig("client").AsString())
	assert.Equal(t, "test_db", root.Dig("db").AsString())
	assert.Equal(t, "test_user", root.Dig("user").AsString())
	assert.Equal(t, "listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\"\n", root.Dig("log").AsString())
}
