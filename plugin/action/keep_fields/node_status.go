package keep_fields

type nodeStatus int

const (
	saved nodeStatus = iota
	parentOfSaved
	unsaved
)
