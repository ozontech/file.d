# JSON decode action plugin
Plugin decodes JSON string from event field and merges result with event root.
If decoded JSON isn't an object, event will be skipped.

## Config params
### field

`cfg.FieldSelector`   

Field of event to use as JSON strings?

### prefix

`string`   

Prefix to add to keys of decoded object.


##
 *Generated using **insane-doc***