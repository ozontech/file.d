package decoder

import (
	"bytes"
	"fmt"

	insaneJSON "github.com/vitkovskii/insane-json"
)

const (
	logDelimiter             = ' '
	pidInfoOpenBrace         = '['
	pidInfoCloseBrace        = ']'
	credentialValueDelimiter = '='
	credentialsDelimiter     = ','
)

func DecodePostgres(event *insaneJSON.Root, data []byte) error {
	// time
	pos := bytes.IndexByte(data, logDelimiter)
	if pos < 0 {
		return fmt.Errorf("timestamp is not found")
	}
	time := data[:pos]
	time = append(time, ' ')
	data = data[pos+1:]

	pos = bytes.IndexByte(data, logDelimiter)
	if pos < 0 {
		return fmt.Errorf("timestamp is not found")
	}
	time = append(time, data[:pos]...)
	time = append(time, ' ')
	data = data[pos+1:]

	pos = bytes.IndexByte(data, logDelimiter)
	if pos < 0 {
		return fmt.Errorf("timestamp is not found")
	}
	time = append(time, data[:pos]...)
	data = data[pos+1:]

	// pid
	pos = bytes.IndexByte(data, pidInfoCloseBrace)
	if pos < 0 {
		return fmt.Errorf("pid is not found")
	}

	pid := data[1:pos]
	data = data[pos+1:]

	// pid message number
	pos = bytes.IndexByte(data, pidInfoOpenBrace)
	if pos < 0 {
		return fmt.Errorf("pid message number start is not found")
	}
	data = data[pos+1:]

	pos = bytes.IndexByte(data, pidInfoCloseBrace)
	if pos < 0 {
		return fmt.Errorf("pid message number end is not found")
	}

	pidMessageNumber := data[:pos]
	data = data[pos+1:]

	// client
	openPos := bytes.IndexByte(data, credentialValueDelimiter)
	if openPos < 0 {
		return fmt.Errorf("client start is not found")
	}

	pos = bytes.IndexByte(data, credentialsDelimiter)
	if pos < 0 {
		return fmt.Errorf("client end is not found")
	}

	client := data[openPos+1 : pos]
	data = data[pos+1:]

	// db
	openPos = bytes.IndexByte(data, credentialValueDelimiter)
	if openPos < 0 {
		return fmt.Errorf("db start is not found")
	}

	pos = bytes.IndexByte(data, credentialsDelimiter)
	if pos < 0 {
		return fmt.Errorf("db end is not found")
	}

	db := data[openPos+1 : pos]
	data = data[pos+1:]

	// user
	openPos = bytes.IndexByte(data, credentialValueDelimiter)
	if openPos < 0 {
		return fmt.Errorf("user start is not found")
	}

	pos = bytes.IndexByte(data, logDelimiter)
	if pos < 0 {
		return fmt.Errorf("user end is not found")
	}

	user := data[openPos+1 : pos]
	data = data[pos+1:]

	// log
	pos = bytes.IndexByte(data, logDelimiter)
	if pos < 0 {
		return fmt.Errorf("log is not found")
	}

	log := data[pos+2:]

	event.AddFieldNoAlloc(event, "time").MutateToBytesCopy(event, time)
	event.AddFieldNoAlloc(event, "pid").MutateToBytesCopy(event, pid)
	event.AddFieldNoAlloc(event, "pid_message_number").MutateToBytesCopy(event, pidMessageNumber)
	event.AddFieldNoAlloc(event, "client").MutateToBytesCopy(event, client)
	event.AddFieldNoAlloc(event, "db").MutateToBytesCopy(event, db)
	event.AddFieldNoAlloc(event, "user").MutateToBytesCopy(event, user)
	event.AddFieldNoAlloc(event, "log").MutateToBytesCopy(event, log)

	return nil
}
