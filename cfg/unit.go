package cfg

const (
	// Decimal

	KB = 1000
	MB = 1000 * KB
	GB = 1000 * MB
	TB = 1000 * GB
	PB = 1000 * TB

	// Binary

	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
	PiB = 1024 * TiB
)

// Use map to add alias.
// Alias must not contain spaces

var UnitAlias = map[string]int{
	"KB": KB, "KiB": KiB,
	"MB": MB, "MiB": MiB,
	"GB": GB, "GiB": GiB,
	"TB": TB, "TiB": TiB,
	"PB": PB, "PiB": PiB,
}
