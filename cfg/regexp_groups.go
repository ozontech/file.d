package cfg

import "go.uber.org/zap"

func isGroupsUnique(groups []int) bool {
	uniqueGrp := make(map[int]struct{}, len(groups))
	var exists struct{}
	for _, g := range groups {
		if _, isContains := uniqueGrp[g]; isContains {
			return false
		}
		uniqueGrp[g] = exists
	}
	return true
}

func VerifyGroupNumbers(groups []int, totalGroups int, logger *zap.Logger) []int {
	if !isGroupsUnique(groups) {
		logger.Fatal("groups numbers must be unique", zap.Ints("groups_numbers", groups))
	}

	if len(groups) > totalGroups {
		logger.Fatal("there are many groups", zap.Int("groups", len(groups)), zap.Int("total_groups", totalGroups))
	}

	for _, g := range groups {
		if g > totalGroups || g < 0 {
			logger.Fatal("wrong group number", zap.Int("number", g))
		} else if g == 0 {
			return []int{0}
		}
	}
	return groups
}
