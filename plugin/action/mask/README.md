# Keep fields plugin
Mask plugin matches event with regular expression and substitutions successfully matched symbols via asterix symbol.
You could set regular expressions and submatch groups.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      masks:
      - mask:
        re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```


### Config params
**`masks`** *`[]Mask`* 

List of masks

<br>

**`re`** *`string`* *`required`* 

Regular expression for masking

<br>

**`groups`** *`[]int`* *`required`* 


<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*