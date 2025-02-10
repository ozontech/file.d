package normalize

type Normalizer interface {
	Normalize(out, data []byte) []byte
}

func formatPlaceholder(out []byte, name string) []byte {
	out = append(out, '<')
	out = append(out, name...)
	out = append(out, '>')
	return out
}
