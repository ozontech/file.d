# Antispam

In some systems services might explode with logs due to different circumstances. If there are a lot of services to collect logs from and some of them suddenly start writing too much logs while the others operate normally, antispam system can help reduce the impact from the spamming services on the others. Usually it is used when there is no room for increasing File.d throughput or capacity, e.g. when File.d is used as daemonset on k8s nodes with limited resources.

## Antispammer

The main entity is `Antispammer`. It counts input data from the sources (e.g. if data comes from [file input plugin](/plugin/input/file/README.md), source can be filename) and decides whether to ban it or not. For each source it counts how many logs it has got, in other words the counter for the source is incremented for each incoming log. When the counter is greater or equal to the threshold value, the source is banned and its counter is set to `unbanIterations * threshold` (where `unbanIterations = 4`). The source remains banned until its counter falls below the `threshold`. Additionally, during each maintenance interval, if the counter is found to be greater than `unbanIterations * threshold`, it is also reset to this maximum value. The counter value is then decremented by the threshold once per maintenance interval.

## Rules

Antispammer has rules which can be applied by checking source name, field in metadata map or log as raw bytes contents. Antispammer iterates through the rules, checks the event and applies the first matching rule.
If event does not match any rule it will be limited with common threshold.

### Rule parameters

**`name`** **`string`**

Name of the rule. If set to nonempty string, adds label value for the `name` label in the `antispam_exceptions` metric.

<br>

**`threshold`** **`int`**

Common threshold applied to events that don't match any rule.
Values:
- `-1` - no limit;
- `0` - discard all logs;
- `> 0` - normal threshold value.

<br>

**`do_if`**

Condition tree. Checks if the event matches the rule(see [doc](/pipeline/doif/README.md)).

> **Note:**
> In the current implementation for this specific context, only the following Do If node types are supported:
> * **`field_op`**
> * **`logical_op`**
>
> Within a `field_op` node, the field path can only reference the following allowed paths:
> * `source_name` — the event source name.
> * `event` — the event content.
> * `meta.field_name` — where `field_name` is a field name within the `meta` object.

<br>

## Exceptions

Antispammer has some exception rules which can be applied by checking source name or log as raw bytes contents. If the log is matched by the rules it is not accounted for in the antispammer. It might be helpful for the logs from critical infrastructure services which must not be banned at all.

> ⚠ DEPRECATED. Use `rules` instead.

### Exception parameters

The exception parameters are the extension of [RuleSet](/cfg/matchrule/README.md).

**`name`** *`string`*

The name of the ruleset of the exception. If set to nonempty string, adds label value for the `name` label in the `antispam_exceptions` metric.

<br>

**`cond`** *`string`* *`default=and`* *`options=and|or`*

Logical conditional operation to combine rules with. If set to `and` exception will only match when all rules are matched. If set to `or` exception will match when at least one of the rules is matched.

<br>

**`rules`** *`[]`Rule*

List of rules to check the log against.

<br>

**`check_source_name`** *`bool`* *`default=false`*

Flag indicating whether to check source name. If set to `true` source name will be checked against all rules. If set to `false` log as raw bytes content will be checked against all rules.

<br>

## Banned sources sample

By default when a source hits the antispam threshold it is banned and all its subsequent logs are silently dropped until unban. That makes debugging a constantly blocked service impossible. The optional block `banned_sources_sample` enables a per-source sampler that lets a small share of banned logs through. Each banned source has its own sampler state: in every `interval` window the first `first` logs pass, then every `thereafter`-th log passes, the rest are dropped. Each passed event can be marked with a boolean field so you can tell sampled events apart from default ones.

### Sample parameters

**`interval`** *`duration`* *`required`*

Sampler window per source.

<br>

**`first`** *`int`*

Number of events from a banned source that always pass at the start of each `interval` window.

<br>

**`thereafter`** *`int`*

After `first` events in the window have passed, every `thereafter`-th event is let through. Set to zero to drop everything past `first`.

<br>

**`sampled_field`** *`string`*

Field to add to log if it was let through the banned sampler. E.g. with `sampled_field: _antispam_sampled`, if the log was sampled, the output
event will have field `"_antispam_sampled":true`. Only works if the `banned_sources_sample` block is set. Useful for marking sampled logs.

<br>

**`sampled_metric_name`** *`string`*

Name of the metric registered for events that passed through the banned sampler.

<br>

**`sampled_metric_labels`** *`[]string`*

Lists the log fields to add to the metric. Blank list means no labels.

<br>
