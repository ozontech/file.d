package journalctl

const (
	subsystemName = "input_journalctl"

	//nolint // reports it's not in use, but it's in use under the build tag 'linux'
	offsetErrors = "offset_errors"
	readerErrors = "reader_errors"
	//nolint // reports it's not in use, but it's in use under the build tag 'linux'
	journalCtlStopErrors = "journalctl_stop_errors"
)
