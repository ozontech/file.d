# Fake plugin
Provides API to test pipelines and other plugins.

> No config params

### API description
``In(sourceID pipeline.SourceID, sourceName string, offset int64, bytes []byte)``

Sends test event into pipeline.

<br>

``SetCommitFn(fn func(event *pipeline.Event))``

Sets up a hook to make sure test event have been successfully committed.

<br>

``SetInFn(fn func())``

Sets up a hook to make sure test event have been passed to plugin.



<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*