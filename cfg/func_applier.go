package cfg

type funcApplier interface {
	tryApply(s string) (string, bool)
}
