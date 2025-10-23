# Mask plugin
Mask plugin matches event with regular expression and substitutions successfully matched symbols via asterix symbol.
You could set regular expressions and submatch groups.

**Note**: masks are applied only to string or number values.

**Example 1:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      masks:
      - re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```

Mask plugin can have white and black lists for fields using `process_fields` and `ignore_fields` parameters respectively.
Elements of `process_fields` and `ignore_fields` lists are json paths (e.g. `message` — field `message` in root,
`field.subfield` — field `subfield` inside object value of field `field`).

**Note**: `process_fields` and `ignore_fields` cannot be used simultaneously.

**Example 2:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      ignore_fields:
      - trace_id
      masks:
      - re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```

All masks will be applied to all fields in the event except for the `trace_id` field in the root of the event.

**Example 3:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      process_fields:
      - message
      masks:
      - re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```

All masks will be applied only to `message` field in the root of the event.

Also `process_fields` and `ignore_fields` lists can be used on per mask basis. In that case, if a mask has
non-empty `process_fields` or `ignore_fields` and there is non-empty `process_fields` or `ignore_fields`
in plugin parameters, mask fields lists will override plugin lists.

**Example 3:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      ignore_fields:
      - trace_id
      masks:
      - re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
      - re: "(test)"
        groups: [1]
		process_fields:
		- message
    ...
```

The first mask will be applied to all fields in the event except for the `trace_id` field in the root of the event.
The second mask will be applied only to `message` field in the root of the event.

### Config params
**`masks`** *`[]Mask`* 

List of masks.

<br>

**`skip_mismatched`** *`bool`* *`default=false`* 

**Deprecated** currently does nothing.

<br>

**`mask_applied_field`** *`string`* 

If any mask has been applied then `mask_applied_field` will be set to `mask_applied_value` in the event.

<br>

**`mask_applied_value`** *`string`* 


<br>

**`ignore_fields`** *`[]string`* 

List of the ignored event fields.
If name of some field contained in this list
then all nested fields will be ignored (even if they are not listed).

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

**`cut_values`** *`bool`* 

CutValues, if set, masking parts will be cut instead of being replaced with ReplaceWord or asterisks.

<br>

**`ignore_fields`** *`[]string`* 

List of the mask-specific ignored event fields.
If name of some field contained in this list
then all nested fields will be ignored (even if they are not listed).
Overrides plugin process/ignore fields lists for the mask.

<br>

**`process_fields`** *`[]string`* 

List of the mask-specific processed event fields.
If name of some field contained in this list
then all nested fields will be processed (even if they are not listed).
If ignored fields list is empty and processed fields list is empty
we consider this as empty ignored fields list (all fields will be processed).
It is wrong to set non-empty ignored fields list and non-empty processed fields list at the same time.
Overrides plugin process/ignore fields lists for the mask.

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