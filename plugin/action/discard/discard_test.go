package discard

import (
	"regexp"
	"sync"
	"testing"

	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestDiscardAnd(t *testing.T) {
	conds := pipeline.MatchConditions{
		pipeline.MatchCondition{
			Field: "field1",
			Value: "value1",
		},
		pipeline.MatchCondition{
			Field: "field2",
			Value: "value2",
		},
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, nil, pipeline.MatchModeAnd, conds, false))

	wg := &sync.WaitGroup{}
	wg.Add(10)

	inEvents := 0
	input.SetInFn(func() {
		wg.Done()
		inEvents++
	})

	outEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		wg.Done()
		outEvents = append(outEvents, e)
	})

	input.In(0, "test", 0, []byte(`{"field1":"not_value1"}`))
	input.In(0, "test", 0, []byte(`{"field2":"not_value2"}`))
	input.In(0, "test", 0, []byte(`{"field1":"value1"}`))
	input.In(0, "test", 0, []byte(`{"field2":"value2"}`))
	input.In(0, "test", 0, []byte(`{"field1":"value1","field2":"value2"}`))
	input.In(0, "test", 0, []byte(`{"field3":"value3","field1":"value1","field2":"value2"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 6, inEvents, "wrong in events count")
	assert.Equal(t, 4, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"field1":"not_value1"}`, outEvents[0].Root.EncodeToString(), "wrong event json")
}

func TestDiscardOr(t *testing.T) {
	conds := pipeline.MatchConditions{
		pipeline.MatchCondition{
			Field: "field1",
			Value: "value1",
		},
		pipeline.MatchCondition{
			Field: "field2",
			Value: "value2",
		},
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, nil, pipeline.MatchModeOr, conds, false))

	wg := &sync.WaitGroup{}
	wg.Add(8)

	inEvents := 0
	input.SetInFn(func() {
		wg.Done()
		inEvents++
	})

	outEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		wg.Done()
		outEvents = append(outEvents, e)
	})

	input.In(0, "test.log", 0, []byte(`{"field1":"not_value1"}`))
	input.In(0, "test.log", 0, []byte(`{"field2":"not_value2"}`))
	input.In(0, "test.log", 0, []byte(`{"field1":"value1"}`))
	input.In(0, "test.log", 0, []byte(`{"field2":"value2"}`))
	input.In(0, "test.log", 0, []byte(`{"field1":"value1","field2":"value2"}`))
	input.In(0, "test.log", 0, []byte(`{"field3":"value3","field1":"value1","field2":"value2"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 6, inEvents, "wrong in events count")
	assert.Equal(t, 2, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"field1":"not_value1"}`, outEvents[0].Root.EncodeToString(), "wrong event json")
}

func TestDiscardRegex(t *testing.T) {
	conds := pipeline.MatchConditions{
		pipeline.MatchCondition{
			Field:  "field1",
			Regexp: regexp.MustCompile("(one|two|three)"),
		},
		pipeline.MatchCondition{
			Field:  "field2",
			Regexp: regexp.MustCompile("four"),
		},
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, nil, pipeline.MatchModeOr, conds, false))

	wg := &sync.WaitGroup{}
	wg.Add(9)

	inEvents := 0
	input.SetInFn(func() {
		wg.Done()
		inEvents++
	})

	outEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		wg.Done()
		outEvents = append(outEvents, e)
	})

	input.In(0, "test.log", 0, []byte(`{"field1":"0000 one 0000"}`))
	input.In(0, "test.log", 0, []byte(`{"field2":"0000 one 0000"}`))
	input.In(0, "test.log", 0, []byte(`{"field1":". two ."}`))
	input.In(0, "test.log", 0, []byte(`{"field1":"four"}`))
	input.In(0, "test.log", 0, []byte(`{"field2":"... four ....","field2":"value2"}`))
	input.In(0, "test.log", 0, []byte(`{"field3":"value3","field1":"value1","field2":"value2"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 6, inEvents, "wrong in events count")
	assert.Equal(t, 3, len(outEvents), "wrong out events count")
}

func TestDiscardMatchInvert(t *testing.T) {
	// only this value should appear
	conds := pipeline.MatchConditions{
		pipeline.MatchCondition{
			Field: "field2",
			Value: "value2",
		},
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, nil, pipeline.MatchModeAnd, conds, true))

	wg := &sync.WaitGroup{}
	wg.Add(9)

	inEvents := 0
	input.SetInFn(func() {
		wg.Done()
		inEvents++
	})

	outEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		wg.Done()
		outEvents = append(outEvents, e)
	})

	input.In(0, "test", 0, []byte(`{"field1":"not_value1"}`))
	input.In(0, "test", 0, []byte(`{"field2":"not_value2"}`))
	input.In(0, "test", 0, []byte(`{"field1":"value1"}`))
	input.In(0, "test", 0, []byte(`{"field2":"value2"}`))
	input.In(0, "test", 0, []byte(`{"field1":"value1","field2":"value2"}`))
	input.In(0, "test", 0, []byte(`{"field3":"value3","field1":"value1","field2":"value2"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 6, inEvents, "wrong in events count")
	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"field2":"value2"}`, outEvents[0].Root.EncodeToString(), "wrong event json")
}
