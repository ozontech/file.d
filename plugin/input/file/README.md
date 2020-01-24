# File input plugin
Plugin is watching for files in the provided directory and reads them line by line.
Each line should contain only one event. It also correctly handles rotations (rename/truncate) and symlinks.
From time to time it instantly releases and reopens descriptors of completely processed files.
Such behaviour allows files to be deleted by third party software even though `file-d` is still working (in this case reopen will fail).
Watcher is trying to use file system events detect file creation and updates.
But update events don't work with symlinks, so watcher also manually `fstat` all tracking files to detect changes.

**Config example for reading docker container log files:**
```yaml
pipelines:
  example_docker_pipeline:
    type: file
    watching_dir: /var/lib/docker/containers
    offsets_file: /data/offsets.yaml
    filename_pattern: "*-json.log"
    persistence_mode: async
```

## Config params
### watching_dir

 `string`  `required`  <br> <br> Directory to watch for new files.
### offsets_file

`string`  `required` 

File name to store offsets of processing files. Offsets are loaded only on initialization.
> It's simply a `yaml` file. You can modify it manually.

### filename_pattern

`string` `default=*`  

Files which doesn't match this pattern will be ignored.
> Check out https://golang.org/pkg/path/filepath/#Glob for details.

### persistence_mode

`string` `default=async`  `options=async|sync`

Defines how to save the offsets file:
*  `async` – periodically saves offsets using `async_interval`. Saving is skipped if offsets haven't been changed. Suitable in most cases, guarantees at least once delivery and makes almost no overhead.
*  `sync` – saves offsets as part of event commitment. It's very slow, but excludes possibility of events duplication in extreme situations like power loss.

Saving is done in three steps:
* Write temporary file with all offsets
* Call `fsync()` on it
* Rename temporary file to the original one

### read_buffer_size

`int` `default=131072`  

Size of buffer to use for file reading.
> Each worker use own buffer, so final memory consumption will be `read_buffer_size*workers_count`.

### max_files

`int` `default=16384`  

Max amount of opened files. If the limit is exceeded `file-d` will exit with fatal.
> Also check your system file descriptors limit: `ulimit -n`.

### offsets_op

`string` `default=continue`  `options=continue|tail|reset`

Offset operation which will be preformed when adding file as a job:
*  `continue` – use offset file
*  `tail` – set offset to the end of the file
*  `reset` – reset offset to the beginning of the file
> It is only used on initial scan of `watching_dir`. Files which will be caught up later during work, will always use `reset` operation.

### workers_count

`int` `default=16`  

How much workers will be instantiated. Each worker:
* Read files (I/O bound)
* Decode events (CPU bound)
> It's recommended to set it to 4x-8x of CPU cores.

### report_interval

 `fd.Duration` `default=10s`   <br> <br> How often to report statistical information to stdout
### maintenance_interval

`fd.Duration` `default=10s`  

How often to perform maintenance.
For now maintenance consists of two stages:
* Symlinks
* Jobs

Symlinks maintenance detects if underlying file of symlink is changed.
Job maintenance `fstat` tracked files to detect if new portion of data have been written to the file. If job is in `done` state when it releases and reopens file descriptor to allow third party software delete the file.

