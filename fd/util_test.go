package fd

import (
	"testing"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/require"
)

func Test_extractConditions(t *testing.T) {
	j, err := simplejson.NewJson([]byte(`{"service": ["address-api", "tarifficator-api", "card-api", "teams-api"]}`))
	require.NoError(t, err)
	got, err := extractConditions(j)
	require.NoError(t, err)
	expected := pipeline.MatchConditions{
		pipeline.MatchCondition{
			Field:  []string{"service"},
			Values: []string{"address-api", "tarifficator-api", "card-api", "teams-api"},
		},
	}
	require.Equal(t, expected, got)

	j, err = simplejson.NewJson([]byte(`{"service": "address-api"}`))
	require.NoError(t, err)
	got, err = extractConditions(j)
	require.NoError(t, err)
	expected = pipeline.MatchConditions{
		pipeline.MatchCondition{
			Field:  []string{"service"},
			Values: []string{"address-api"},
		},
	}
	require.Equal(t, expected, got)
}
