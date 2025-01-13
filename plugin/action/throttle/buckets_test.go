package throttle

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetaActualizeIndex(t *testing.T) {
	type testCase struct {
		curMaxID int
		newMaxID int
		index    int
	}

	tests := []struct {
		name string
		tc   testCase

		wantIndex  int
		wantActual bool
	}{
		{
			name: "same_max_id",
			tc: testCase{
				curMaxID: 10,
				newMaxID: 10,
				index:    5,
			},
			wantIndex:  5,
			wantActual: true,
		},
		{
			name: "not_same_max_id_actual",
			tc: testCase{
				curMaxID: 12,
				newMaxID: 10,
				index:    5,
			},
			wantIndex:  3,
			wantActual: true,
		},
		{
			name: "not_same_max_id_not_actual",
			tc: testCase{
				curMaxID: 30,
				newMaxID: 10,
				index:    5,
			},
			wantIndex:  -15,
			wantActual: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			meta := bucketsMeta{
				maxID: tt.tc.curMaxID,
			}
			index, actual := meta.actualizeIndex(tt.tc.newMaxID, tt.tc.index)
			require.Equal(t, tt.wantIndex, index, "wrong index")
			require.Equal(t, tt.wantActual, actual, "wrong actuality")
		})
	}
}

func TestRebuildBuckets(t *testing.T) {
	const interval = time.Second

	// equals to current bucketsMeta.timeToBucketID func
	timeToID := func(t time.Time) int {
		return int(t.UnixNano() / interval.Nanoseconds())
	}

	ts, _ := time.Parse(time.RFC3339, "2024-02-12T10:20:30Z")

	type testCase struct {
		b         []int64
		meta      *bucketsMeta
		currentTs time.Time
		ts        time.Time
	}
	type wantData struct {
		id      int
		buckets []int64
		minID   int
		maxID   int
	}

	tests := []struct {
		name string
		tc   testCase
		want wantData
	}{
		{
			name: "zero_min_id",
			tc: testCase{
				b: []int64{1, 2, 3},
				meta: &bucketsMeta{
					count:    3,
					interval: interval,
					minID:    0,
				},
				currentTs: ts,
			},
			want: wantData{
				id:      -1,
				buckets: []int64{1, 2, 3},
				minID:   timeToID(ts) - 2, // 2 = count-1
				maxID:   timeToID(ts),
			},
		},
		{
			name: "current_id_not_greater_max_id",
			tc: testCase{
				b: []int64{1, 2, 3},
				meta: &bucketsMeta{
					count:    3,
					interval: interval,
					minID:    timeToID(ts),
				},
				currentTs: ts,
			},
			want: wantData{
				id:      -1,
				buckets: []int64{1, 2, 3},
				minID:   timeToID(ts),
				maxID:   -1,
			},
		},
		{
			name: "current_id_greater_max_id",
			tc: testCase{
				b: []int64{1, 2, 3},
				meta: &bucketsMeta{
					count:    3,
					interval: interval,
					minID:    timeToID(ts),
				},
				currentTs: ts.Add(4 * time.Second),
			},
			want: wantData{
				id:      -1,
				buckets: []int64{3, 0, 0},
				minID:   timeToID(ts.Add(2 * time.Second)),
				maxID:   timeToID(ts.Add(4 * time.Second)),
			},
		},
		{
			name: "ts_id_between_minmax",
			tc: testCase{
				meta: &bucketsMeta{
					interval: interval,
					minID:    timeToID(ts),
					maxID:    timeToID(ts.Add(3 * time.Second)),
				},
				ts: ts.Add(time.Second),
			},
			want: wantData{
				id:    timeToID(ts.Add(time.Second)),
				minID: -1,
				maxID: -1,
			},
		},
		{
			name: "ts_id_not_between_minmax",
			tc: testCase{
				meta: &bucketsMeta{
					interval: interval,
					minID:    timeToID(ts),
					maxID:    timeToID(ts.Add(3 * time.Second)),
				},
				ts: ts.Add(5 * time.Second),
			},
			want: wantData{
				id:    timeToID(ts.Add(3 * time.Second)), // same as maxID
				minID: -1,
				maxID: -1,
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resetFn := func(count int) {
				tt.tc.b = append(tt.tc.b[count:], tt.tc.b[:count]...)
				for i := 0; i < count; i++ {
					tt.tc.b[len(tt.tc.b)-1-i] = 0
				}
			}
			id := rebuildBuckets(tt.tc.meta, resetFn, tt.tc.currentTs, tt.tc.ts)

			if tt.want.id != -1 {
				require.Equal(t, tt.want.id, id, "wrong ID")
			}
			if len(tt.want.buckets) > 0 {
				require.Equal(t, true, slices.Equal(tt.want.buckets, tt.tc.b), "wrong buckets")
			}
			if tt.want.minID != -1 {
				require.Equal(t, tt.want.minID, tt.tc.meta.minID, "wrong buckets min ID")
			}
			if tt.want.maxID != -1 {
				require.Equal(t, tt.want.maxID, tt.tc.meta.maxID, "wrong buckets max ID")
			}
		})
	}
}
