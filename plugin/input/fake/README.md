# Fake input plugin
Plugin provides methods to use in test scenarios:

``In(sourceID pipeline.SourceID, sourceName string, offset int64, _ int64, bytes []byte)``<br>Sends test event into pipeline.
<br><br>
``SetCommitFn(fn func(event *pipeline.Event))``<br>Sets up a hook to make sure test event have been successfully committed.
<br><br>
``SetInFn(fn func())``<br>Sets up a hook to make sure test event have been passed to plugin.


> No config params

##
 *Generated using **insane-doc***