# Match rules

Match rules are lightweight checks for the raw byte contents. The rules are combined in rulesets, they can be used with logical `and` or `or` applied to all rules in the rulset, its result might be inverted, they can check values in case insensitive mode.

## Rule

**`values`** *`[]string`*

List of values to check the content against.

<br>

**`mode`** *`string`* *`required`* *`options=prefix|suffix|contains`*

Content check mode. In `prefix` mode only first bytes of the content are checked. In `suffix` mode only last bytes of the content are checked. In `contains` mode there is a substring search in the contents.

<br>

**`case_insensitive`** *`bool`* *`default=false`*

When `case_insensitive` is set to `true` all `values` and the checking contents are converted to lowercase. It is better to avoid using this mode because it can impact throughput and performance of the logs collection. 

<br>

**`invert`** *`bool`* *`default=false`*

Flag indicating whether to negate the match result. For example if all of the rules are matched and `invert` is set to `true` the whole ruleset will result as not matched. It should be used when it is easier to list items that should not match the rules.

<br>

## RuleSet

**`name`** *`string`*

The name of the ruleset. Has some additional semantics in [antispam exceptions](/pipeline/antispam/README.md#exception-parameters).

<br>

**`cond`** *`string`* *`default=and`* *`options=and|or`*

Logical conditional operation to combine rules with. If set to `and` ruleset will only match when all rules are matched. If set to `or` ruleset will match when at least one of the rules is matched.

<br>

**`rules`** *`[]`[Rule](/cfg/matchrule/README.md#rule)*

List of rules to check the log against.

<br>

## RuleSets

List of [RuleSet](/cfg/matchrule/README.md#ruleset). Always combined with logical `or`, meaning it matches when at least one of the rulesets match.
