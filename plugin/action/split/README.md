# Split plugin
It splits array of objects into different events.

For example:
```json
{
	"data": [
		{ "message": "go" },
		{ "message": "rust" },
		{ "message": "c++" }
	]
}
```

Split produces:
```json
{ "message": "go" },
{ "message": "rust" },
{ "message": "c++" }
```

Parent event will be discarded.
If the value of the JSON field is not an array of objects, then the event will be pass unchanged.

### Config params
**`field`** *`cfg.FieldSelector`* 

Path to the array of objects.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*