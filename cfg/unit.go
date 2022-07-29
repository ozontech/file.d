package cfg

const (
	B = 1

	// KB ,MB ,GB ,TB ,PB are Decimal
	KB = 1000 * B
	MB = 1000 * KB
	GB = 1000 * MB
	TB = 1000 * GB
	PB = 1000 * TB

	// KiB ,MiB ,GiB ,TiB ,PiB are Binary
	KiB = 1024 * B
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
	PiB = 1024 * TiB
)

// UnitAlias is map to add alias.
// Alias must not contain space
// Only lowercase letters should be used in the map, but aliases are case-insensitive
var UnitAlias = map[string]int{
	"kb": KB, "kib": KiB,
	"mb": MB, "mib": MiB,
	"gb": GB, "gib": GiB,
	"tb": TB, "tib": TiB,
	"pb": PB, "pib": PiB,
	"b": B,
}
