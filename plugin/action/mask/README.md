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
      metric_subsystem_name: "some_name"
      masks:
      - mask:
        re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```


### Config params
**`metric_subsystem_name`** *`*string`* 

If set counterMetric with this name would be sent on metric_subsystem_name.mask_plugin

<br>

**`masks`** *`[]Mask`* 

List of masks.

<br>

**`re`** *`string`* *`required`* 

Regular expression for masking

<br>

**`groups`** *`[]int`* *`required`* 

Numbers of masking groups in expression, zero for mask all expression

<br>

**`max_count`** *`int`* 

MaxCount limits the number of masked symbols in the masked output, if zero, no limit is set

<br>

**`replace_word`** *`string`* 

ReplaceWord, if set, is used instead of asterisks for masking patterns that are of the same length or longer.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*