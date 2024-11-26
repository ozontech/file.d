package decoder

import (
	"bytes"
	"fmt"

	insaneJSON "github.com/ozontech/insane-json"
)

const (
	logDelimiter             = ' '
	pidInfoOpenBrace         = '['
	pidInfoCloseBrace        = ']'
	credentialValueDelimiter = '='
	credentialsDelimiter     = ','
)

type PostgresRow struct {
	Time             []byte
	PID              []byte
	PIDMessageNumber []byte
	Client           []byte
	DB               []byte
	User             []byte
	Log              []byte
}

// DecodePostgresToJson decodes postgres formatted log and merges result with event.
//
// From:
//
//	"2021-06-22 16:24:27 GMT [7291] => [3-1] client=test_client,db=test_db,user=test_user LOG:  listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\""
//
// To:
//
//	{
//		"time": "2021-06-22 16:24:27 GMT",
//		"pid": "7291",
//		"pid_message_number": "3-1",
//		"client": "test_client",
//		"db": "test_db",
//		"user": "test_user",
//		"log": "listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\""
//	}
func DecodePostgresToJson(event *insaneJSON.Root, data []byte) error {
	row, err := DecodePostgres(data)
	if err != nil {
		return err
	}

	event.AddFieldNoAlloc(event, "time").MutateToBytesCopy(event, row.Time)
	event.AddFieldNoAlloc(event, "pid").MutateToBytesCopy(event, row.PID)
	event.AddFieldNoAlloc(event, "pid_message_number").MutateToBytesCopy(event, row.PIDMessageNumber)
	event.AddFieldNoAlloc(event, "client").MutateToBytesCopy(event, row.Client)
	event.AddFieldNoAlloc(event, "db").MutateToBytesCopy(event, row.DB)
	event.AddFieldNoAlloc(event, "user").MutateToBytesCopy(event, row.User)
	event.AddFieldNoAlloc(event, "log").MutateToBytesCopy(event, row.Log)

	return nil
}

// DecodePostgres decodes postgres formated log to [PostgresRow].
//
// Example of format:
//
//	"2021-06-22 16:24:27 GMT [7291] => [3-1] client=test_client,db=test_db,user=test_user LOG:  listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\""
func DecodePostgres(data []byte) (PostgresRow, error) {
	row := PostgresRow{}

	// time
	pos := bytes.IndexByte(data, logDelimiter)
	if pos < 0 {
		return row, fmt.Errorf("timestamp is not found")
	}
	time := data[:pos]
	time = append(time, ' ')
	data = data[pos+1:]

	pos = bytes.IndexByte(data, logDelimiter)
	if pos < 0 {
		return row, fmt.Errorf("timestamp is not found")
	}
	time = append(time, data[:pos]...)
	time = append(time, ' ')
	data = data[pos+1:]

	pos = bytes.IndexByte(data, logDelimiter)
	if pos < 0 {
		return row, fmt.Errorf("timestamp is not found")
	}
	time = append(time, data[:pos]...)
	data = data[pos+1:]

	row.Time = time

	// pid
	pos = bytes.IndexByte(data, pidInfoCloseBrace)
	if pos < 0 {
		return row, fmt.Errorf("pid is not found")
	}

	row.PID = data[1:pos]
	data = data[pos+1:]

	// pid message number
	pos = bytes.IndexByte(data, pidInfoOpenBrace)
	if pos < 0 {
		return row, fmt.Errorf("pid message number start is not found")
	}
	data = data[pos+1:]

	pos = bytes.IndexByte(data, pidInfoCloseBrace)
	if pos < 0 {
		return row, fmt.Errorf("pid message number end is not found")
	}

	row.PIDMessageNumber = data[:pos]
	data = data[pos+1:]

	// client
	openPos := bytes.IndexByte(data, credentialValueDelimiter)
	if openPos < 0 {
		return row, fmt.Errorf("client start is not found")
	}

	pos = bytes.IndexByte(data, credentialsDelimiter)
	if pos < 0 {
		return row, fmt.Errorf("client end is not found")
	}

	row.Client = data[openPos+1 : pos]
	data = data[pos+1:]

	// db
	openPos = bytes.IndexByte(data, credentialValueDelimiter)
	if openPos < 0 {
		return row, fmt.Errorf("db start is not found")
	}

	pos = bytes.IndexByte(data, credentialsDelimiter)
	if pos < 0 {
		return row, fmt.Errorf("db end is not found")
	}

	row.DB = data[openPos+1 : pos]
	data = data[pos+1:]

	// user
	openPos = bytes.IndexByte(data, credentialValueDelimiter)
	if openPos < 0 {
		return row, fmt.Errorf("user start is not found")
	}

	pos = bytes.IndexByte(data, logDelimiter)
	if pos < 0 {
		return row, fmt.Errorf("user end is not found")
	}

	row.User = data[openPos+1 : pos]
	data = data[pos+1:]

	// log
	pos = bytes.IndexByte(data, logDelimiter)
	if pos < 0 {
		return row, fmt.Errorf("log is not found")
	}

	row.Log = data[pos+2:]

	return row, nil
}
