# File plugin
Plugin is watching for files in the provided directory and reads them line by line.
Each line should contain only one event. It also correctly handles rotations (rename/truncate) and symlinks.
From time to time it instantly releases and reopens descriptors of completely processed files.
Such behaviour allows files to be deleted by third party software even though `file-d` is still working(in this case reopen will fail).
Watcher trying to use file system events to watch for files.
But events isn't work with symlinks, so watcher also manually rescans directory by some interval.

**Config example:**
```yaml
pipelines:
  docker:
    type: file
    persistence_mode: async
    watching_dir: /var/lib/docker/containers
    filename_pattern: "*-json.log"
    offsets_file: /data/offsets.yaml
```

## Config params
### watching_dir 
 `string` `required` <br> <br> Directory to watch for new files.
### offsets_file 
 `string` `required`

File name to store offsets of processing files. Offsets are loaded only on initialization.
> It's simply a `yaml` file. You can manually modify it manually.
### filename_pattern 
 `string` `default="*"`

Files which doesn't match this pattern will be ignored.
> Check out https://golang.org/pkg/path/filepath/#Glob for details.
### persistence_mode 
 `string="sync|async"` `default="async"`

Defines how to save the offsets file:
* `sync` – saves offsets as part of event commitment. It's very slow, but excludes possibility of events duplication in extreme situations like power loss.
* `async` – periodically saves offsets using `persist_interval`. Saving is skipped if offsets haven't been changed. Suitable in most cases, guarantees at least once delivery and makes almost no overhead.

Saving is done in three steps:
* Write temporary file with all offsets
* Call `fsync()` on it
* Rename temporary file to the original one
### persist_interval 
 `time` `default=time.Second` <br> <br> Offsets save interval. Only used if `persistence_mode` is `async`.
### read_buffer_size 
 `number` `default=131072`

Size of buffer to use for file reading.
> Each worker use own buffer so final memory consumption will be `read_buffer_size*workers_count`.
### max_files 
 `number` `default=16384`

Max amount of opened files. If the limit is exceeded `file-d` will exit with fatal.
> Also check your system file descriptors limit: `ulimit -n`.
### offsets_op 
 `string="continue|reset|tail"` default=`"continue"`

Offset operation which will be preformed when adding file as a job:
* `continue` – use offset file
* `reset` – reset offset to the beginning of the file
* `tail` – set offset to the end of the file
> It is only used on initial scan of `watching_dir`. Files which will be caught up later during work, will always use `reset` operation.
### workers_count 
 `number` `default=16`

How much workers will be instantiated. Each worker:
* Read files (I/O bound)
* Decode events (CPU bound)
> It's recommended to set it to 4x-8x of CPU cores.

