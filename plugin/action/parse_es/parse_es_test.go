package parse_es

import (
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/ozontech/file.d/pipeline"
)

func TestDoTimeout(t *testing.T) {
	log := zap.NewExample().Sugar()
	p := &Plugin{logger: log}

	event := pipeline.Event{}
	event.SetTimeoutKind()

	action := p.Do(&event)
	assert.Equal(t, pipeline.ActionDiscard, action, "timeout action must be discarded, but it wasn't")
}

func TestDoCollapseDescribePanic(t *testing.T) {
	log := zap.NewExample().Sugar()

	p := &Plugin{logger: log}
	p.discardNext = true
	p.passNext = true

	event := pipeline.Event{}

	assert.Panics(t, func() { p.Do(&event) }, &event)
}

func TestDoPassAndDiscard(t *testing.T) {
	type testCase struct {
		name                  string
		passNextStartState    bool
		discardNextStartState bool

		expectedAction        pipeline.ActionResult
		passNextExpectedState bool
		discardExpectedState  bool
	}
	cases := []testCase{
		{
			name:                  "pass_next",
			passNextStartState:    true,
			discardNextStartState: false,

			expectedAction:        pipeline.ActionPass,
			passNextExpectedState: false,
			discardExpectedState:  false,
		},

		{
			name:                  "discard_next",
			passNextStartState:    false,
			discardNextStartState: true,

			expectedAction:        pipeline.ActionCollapse,
			passNextExpectedState: false,
			discardExpectedState:  false,
		},
	}

	for _, tCase := range cases {
		tCase := tCase
		t.Run(tCase.name, func(t *testing.T) {
			p := &Plugin{}
			p.passNext = tCase.passNextStartState
			p.discardNext = tCase.discardNextStartState

			event := pipeline.Event{}

			action := p.Do(&event)
			assert.Equal(t, tCase.expectedAction, action)
			assert.Equal(t, tCase.passNextExpectedState, p.passNext)
			assert.Equal(t, tCase.discardExpectedState, p.discardNext)
		})
	}
}

func TestDo(t *testing.T) {
	type testCase struct {
		name    string
		digName string

		expectedAction        pipeline.ActionResult
		passNextExpectedState bool
		discardExpectedState  bool
	}
	cases := []testCase{
		{
			name:                  "request_delete",
			digName:               "delete",
			expectedAction:        pipeline.ActionCollapse,
			passNextExpectedState: false,
			discardExpectedState:  false,
		},
		{
			name:                  "request_update",
			digName:               "update",
			expectedAction:        pipeline.ActionCollapse,
			passNextExpectedState: false,
			discardExpectedState:  true,
		},
		{
			name:                  "request_index",
			digName:               "index",
			expectedAction:        pipeline.ActionCollapse,
			passNextExpectedState: true,
			discardExpectedState:  false,
		},
		{
			name:                  "request_create",
			digName:               "create",
			expectedAction:        pipeline.ActionCollapse,
			passNextExpectedState: true,
			discardExpectedState:  false,
		},
	}

	for _, tCase := range cases {
		tCase := tCase
		t.Run(tCase.name, func(t *testing.T) {
			p := &Plugin{}
			root := insaneJSON.Spawn()
			defer insaneJSON.Release(root)

			_ = root.AddField(tCase.digName)

			event := pipeline.Event{Root: root}
			action := p.Do(&event)
			assert.Equal(t, tCase.expectedAction, action)
			assert.Equal(t, tCase.passNextExpectedState, p.passNext)
			assert.Equal(t, tCase.discardExpectedState, p.discardNext)
		})
	}
}

func TestDoDiscardBadRequest(t *testing.T) {
	log := zap.NewExample().Sugar()
	p := &Plugin{logger: log}

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	_ = root.AddField("bad_request_discard_me")

	event := pipeline.Event{Root: root}
	action := p.Do(&event)
	assert.Equal(t, pipeline.ActionDiscard, action)
	assert.Equal(t, false, p.passNext)
	assert.Equal(t, false, p.discardNext)
}
