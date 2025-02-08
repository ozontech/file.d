# Fake plugin
It provides an API to test pipelines and other plugins.

> No config params

### API description
``In(sourceID pipeline.SourceID, sourceName string, offset pipeline.Offsets, bytes []byte)``

It sends a test event into the pipeline.

<br>

``SetCommitFn(fn func(event *pipeline.Event))``

It sets up a hook to make sure the test event has been successfully committed.

<br>

``SetInFn(fn func())``

It sets up a hook to make sure the test event has been passed to the plugin.



<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*