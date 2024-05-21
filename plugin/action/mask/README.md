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
      ignore_fields:
      - trace_id
      masks:
      - mask:
        re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```


### Config params
**`masks`** *`[]Mask`* 

List of masks.

<br>

**`skip_mismatched`** *`bool`* *`default=false`* 

**Experimental feature** for best performance. Skips events with mismatched masks.

<br>

**`mask_applied_field`** *`string`* 

If any mask has been applied then `mask_applied_field` will be set to `mask_applied_value` in the event.

<br>

**`mask_applied_value`** *`string`* 


<br>

**`ignore_fields`** *`[]string`* 

List of the ignored event fields (field depth doesn't matter).
If name of some field contained in this list
then all nested fields will be ignored (even if they are not listed)

<br>

**`process_fields`** *`[]string`* 

List of the processed event fields.
If name of some field contained in this list
then all nested fields will be processed (even if they are not listed).
If ignored fields list is empty and processed fields list is empty
we consider this as empty ignored fields list (all fields will be processed).
It is wrong to set non-empty ignored fields list and non-empty processed fields list at the same time.

<br>

**`applied_metric_name`** *`string`* *`default=mask_applied_total`* 

The metric name of the regular expressions applied.

<br>

**`applied_metric_labels`** *`[]string`* 

Lists the event fields to add to the metric. Blank list means no labels.
Important note: labels metrics are not currently being cleared.

<br>

**`match_rules`** *`matchrule.RuleSets`* 

List of matching rules to filter out events before checking regular expression for masking.

<br>

**`re`** *`string`* 

Regular expression for masking.

<br>

**`groups`** *`[]int`* 

Groups are numbers of masking groups in expression, zero for mask all expression.

<br>

**`max_count`** *`int`* 

MaxCount limits the number of masked symbols in the masked output, if zero, no limit is set.

<br>

**`replace_word`** *`string`* 

ReplaceWord, if set, is used instead of asterisks for masking patterns that are of the same length or longer.

<br>

**`applied_field`** *`string`* 

If the mask has been applied then `applied_field` will be set to `applied_value` in the event.

<br>

**`applied_value`** *`string`* 

Value to be set in `applied_field`.

<br>

**`metric_name`** *`string`* 

The metric name of the regular expressions applied.
The metric name for a mask cannot be the same as metric name for plugin.

<br>

**`metric_labels`** *`[]string`* 

Lists the event fields to add to the metric. Blank list means no labels.
Important note: labels metrics are not currently being cleared.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*