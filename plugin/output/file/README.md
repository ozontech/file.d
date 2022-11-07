# File output
This plugin implements writing to file.
Can be used on its own, but is also part of the s3 plugin.

**An example for discarding informational and debug logs:**
```yaml
pipelines:
  example_pipeline:
    ...
    output:
      type: file
      retention_interval: 3h
      target_file: /var/log/file-d.log
    ...
```

### Config params
**`target_file`** *`string`* *`default=/var/log/file-d.log`* 

File name for log file.
defaultTargetFileName = TargetFile default value

<br>

**`retention_interval`** *`cfg.Duration`* *`default=1h`* 

Interval of creation new file

<br>

**`retention_size`** *`cfg.DataUnit`* *`default=1 PB`* 

Interval of creation new file (by file size)

<br>

**`time_layout`** *`string`* *`default=01-02-2006_15:04:05`* 

Layout is added to targetFile after sealing up. Determines result file name

<br>

**`workers_count`** *`cfg.Expression`* *`default=gomaxprocs*4`* 

How much workers will be instantiated to send batches.

<br>

**`batch_size`** *`cfg.Expression`* *`default=capacity/4`* 

Maximum quantity of events to pack into one batch.

<br>

**`batch_size_bytes`** *`cfg.Expression`* *`default=0`* 

A minimum size of events in a batch to send.
If both batch_size and batch_size_bytes are set, they will work together.

<br>

**`batch_flush_timeout`** *`cfg.Duration`* *`default=1s`* 

After this timeout batch will be sent even if batch isn't completed.

<br>

**`file_mode`** *`cfg.Base8`* *`default=0666`* 

File mode for log files

<br>

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*