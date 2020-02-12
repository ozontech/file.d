# JSON decode action plugin
Plugin decodes JSON string from event field and merges result with event root.
If decoded JSON isn't an object, event will be skipped.

## Config params
- **`field`** *`cfg.FieldSelector`*   *`required`*  

Field of event to decode. Should be string.
<br><br>

- **`prefix`** *`string`*    

Prefix to add to keys of decoded object.
<br><br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*