# File plugin
It watches for files in the provided directory and reads them line by line.

Each line should contain only one event. It also correctly handles rotations (rename/truncate) and symlinks.

From time to time, it instantly releases and reopens descriptors of the completely processed files.
Such behavior allows files to be deleted by a third party software even though `file.d` is still working (in this case the reopening will fail).

A watcher is trying to use the file system events to detect file creation and updates.
But update events don't work with symlinks, so watcher also periodically manually `fstat` all tracking files to detect changes.

> ⚠ It supports the commitment mechanism. But "least once delivery" is guaranteed only if files aren't being truncated.
> However, `file.d` correctly handles file truncation, there is a little chance of data loss.
> It isn't a `file.d` issue. The data may have been written just before the file truncation. In this case, you may miss to read some events.
> If you care about the delivery, you should also know that the `logrotate` manual clearly states that copy/truncate may cause data loss even on a rotating stage.
> So use copy/truncate or similar actions only if your data isn't critical.
> In order to reduce potential harm of truncation, you can turn on notifications of file changes.
> By default the plugin is notified only on file creations. Note that following for changes is more CPU intensive.

> ⚠ Use add_file_name plugin if you want to add filename to events.

**Reading docker container log files:**
```yaml
pipelines:
  example_docker_pipeline:
    input:
        type: file
        paths:
          include:
            - '/var/lib/docker/containers/**\/*-json.log' # remove \
          exclude:
            - '/var/lib/docker/containers/19aa5027343f4*\/*-json.log' # remove \
        offsets_file: /data/offsets.yaml
        persistence_mode: async
```

### Config params
**`watching_dir`** *`string`* *`required`* 

List of included pathes
*`string`* *`required`* 

List of excluded pathes
*`string`* *`required`* 

The source directory to watch for files to process. All subdirectories also will be watched. E.g. if files have
`/var/my-logs/$YEAR/$MONTH/$DAY/$HOST/$FACILITY-$PROGRAM.log` structure, `watching_dir` should be `/var/my-logs`.
Also the `filename_pattern`/`dir_pattern` is useful to filter needless files/subdirectories. In the case of using two or more
different directories, it's recommended to setup separate pipelines for each.

<br>

**`paths`** *`Paths`* 

Paths.
> Check out [func Glob docs](https://golang.org/pkg/path/filepath/#Glob) for details.

<br>

**`offsets_file`** *`string`* *`required`* 

The filename to store offsets of processed files. Offsets are loaded only on initialization.
> It's a `yaml` file. You can modify it manually.

<br>

**`filename_pattern`** *`string`* *`default=*`* 

Files that don't meet this pattern will be ignored.
> Check out [func Glob docs](https://golang.org/pkg/path/filepath/#Glob) for details.

<br>

**`dir_pattern`** *`string`* *`default=*`* 

Dirs that don't meet this pattern will be ignored.
> Check out [func Glob docs](https://golang.org/pkg/path/filepath/#Glob) for details.

<br>

**`persistence_mode`** *`string`* *`default=async`* *`options=async|sync`* 

It defines how to save the offsets file:
*  `async` – it periodically saves the offsets using `async_interval`. The saving operation is skipped if offsets haven't been changed. Suitable, in most cases, it guarantees at least one delivery and makes almost no overhead.
*  `sync` – saves offsets as part of event commitment. It's very slow but excludes the possibility of event duplication in extreme situations like power loss.

Save operation takes three steps:
*  Write the temporary file with all offsets;
*  Call `fsync()` on it;
*  Rename the temporary file to the original one.

<br>

**`async_interval`** ! *`cfg.Duration`*  *`default=1s`*    <br> <br> Offsets saving interval. Only used if `persistence_mode` is set to `async`.
<br>

**`read_buffer_size`** *`int`* *`default=131072`* 

The buffer size used for the file reading.
> Each worker uses its own buffer so that final memory consumption will be `read_buffer_size*workers_count`.

<br>

**`max_files`** *`int`* *`default=16384`* 

The max amount of opened files. If the limit is exceeded, `file.d` will exit with fatal.
> Also, it checks your system's file descriptors limit: `ulimit -n`.

<br>

**`offsets_op`** *`string`* *`default=continue`* *`options=continue|tail|reset`* 

An offset operation which will be performed when you add a file as a job:
*  `continue` – uses an offset file
*  `tail` – sets an offset to the end of the file
*  `reset` – resets an offset to the beginning of the file
> It is only used on an initial scan of `watching_dir`. Files that will be caught up later during work will always use `reset` operation.

<br>

**`workers_count`** *`cfg.Expression`* *`default=gomaxprocs*8`* 

It defines how many workers will be instantiated.
Each worker:
* Reads files (I/O bound)
* Decodes events (CPU bound)
> We recommend to set it to 4x-8x of CPU cores.

<br>

**`report_interval`** *`cfg.Duration`* *`default=5s`* 

It defines how often to report statistical information to stdout

<br>

**`maintenance_interval`** *`cfg.Duration`* *`default=10s`* 

It defines how often to perform maintenance
For now maintenance consists of two stages:
* Symlinks
* Jobs

Symlinks maintenance detects if underlying file of symlink is changed.
Job maintenance `fstat` tracked files to detect if new portion of data have been written to the file. If job is in `done` state when it releases and reopens file descriptor to allow third party software delete the file.

<br>

**`should_watch_file_changes`** *`bool`* *`default=false`* 

It turns on watching for file modifications. Turning it on cause more CPU work, but it is more probable to catch file truncation

<br>

**`meta`** *`cfg.MetaTemplates`* 

Meta params

Add meta information to an event (look at Meta params)
Use [go-template](https://pkg.go.dev/text/template) syntax

Example: ```filename: '{{ .filename }}'```

<br>


### Meta params
**`filename`** 

**`symlink`** 

**`inode`** 

**`offset`** 

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*