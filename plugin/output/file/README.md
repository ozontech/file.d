# File output
It sends event batches into files.

### Config params
**`target_file`** *`string`* *`default=/var/log/file-d.log`* 

File path to write events to.
Until it's sealed up, the file is named with its creation unix time:
`/var/log/file-d.log` -> `/var/log/1893445200_file-d.log`

The path may contain `${field}` and `%y`, `%m`, `%d`.
For example, if `target_file` is `/var/log/%y/%m/%d/${host}/file-d.log` file will be `/var/log/2006/01/02/host1/1893445200_file-d.log`

<br>

**`retention_interval`** *`cfg.Duration`* *`default=1h`* 

How long to write a file before sealing it up.

<br>

**`compression`** *`string`* *`default=none`* *`options=none|gzip|zstd|lz4`* 

Compressed files format.

<br>

**`time_layout`** *`string`* *`default=2006-01-02 15:04:05`* 

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