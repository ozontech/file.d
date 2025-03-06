package normalize

type Normalizer interface {
	Normalize(out, data []byte) []byte
}
