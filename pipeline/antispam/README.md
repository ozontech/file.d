# Antispam

In some systems services might explode with logs due to different circumstances. If there are a lot of services to collect logs from and some of them suddenly start writing too much logs while the others operate normally, antispam system can help reduce the impact from the spamming services on the others. Usually it is used when there is no room for increasing File.d throughput or capacity, e.g. when File.d is used as daemonset on k8s nodes with limited resources.

## Antispammer

The main entity is `Antispammer`. It counts input data from the sources (e.g. if data comes from [file input plugin](/plugin/input/file/README.md), source can be filename) and decides whether to ban it or not. For each source it counts how many logs it has got, in other words the counter for the source is incremented for each incoming log. When the counter is greater or equal to the threshold value the source is banned until its counter is less than the threshold value. The counter value is decremented once in maintenance interval by the threshold value. The maintenance interval for antispam is the same as for the pipeline (see `maintenance_interval` in [pipeline settings](/pipeline/README.md#settings)).

## Exceptions

Antispammer has some exception rules which can be applied by checking source name or log as raw bytes contents. If the log is matched by the rules it is not accounted for in the antispammer. It might be helpful for the logs from critical infrastructure services which must not be banned at all.

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
