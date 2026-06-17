package normalize

type Normalizer interface {
	Normalize(out, data []byte, cropped bool) []byte
}
