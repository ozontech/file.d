# journald

Reads log entries from the systemd journal (journald daemon).

This plugin is the renamed version of the `journalctl` plugin. Use `type: journald`
in new configurations. The `journalctl` name still works but is deprecated.

## Configuration

See the [journalctl plugin documentation](../journalctl/README.md) — all
configuration options are identical.

## Example

```yaml
pipelines:
  example:
    input:
      type: journald       # preferred
      # type: journalctl   # deprecated alias, still supported
    output:
      type: stdout
```
