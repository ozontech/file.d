# File plugin
Plugin is watching for files in the provided directory and reads them line by line.
Each line should contain only one event. It also correctly handles rotations (rename/truncate) and symlinks.
From time to time it instantly releases and reopens descriptors of completely processed files.
Such behaviour allows files to be deleted by third party software even though `file-d` is still working(in this case reopen will fail).
Watcher trying to use file system events to watch for files.
But events isn't work with symlinks, so watcher also manually rescans directory by some interval.
Config example:
```
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
 `string` `required` <br> <br> Directory to watch for files.
### offsets_file 
 `string` `required` <br> <br> File name into which offsets will be saving. It's simply yaml file. You can read and manually modify it. Offsets are loaded only on initialization.
### filename_pattern 
 `string` `default="*"` <br> <br> Files which doesn't match this pattern will be ignored. Check out https://golang.org/pkg/path/filepath/#Glob for details.
### persistence_mode 
 `string="sync|async"` `default="async"`

Defines how to save offsets file:
* `sync` – saves offsets file with event commitment. It's very slow, but excludes possibility of events duplication in extreme situations like power loss.
* `async` – saves offsets file by timer using `persist_interval`. Saving is skipped if offsets haven't been changed. Suitable in most cases, guarantees at least once delivery and makes almost no overhead.

Save means doing three stages:
* Write temporary file with all offsets
* Call `fsync()` on it
* Rename temporary file to original
### persist_interval 
 `time` `default=time.Second` <br> <br> Offsets save interval. Used only if `persistence_mode` is `async`.
### read_buffer_size 
 `number` `default=131072` <br> <br> Size of buffer to use for file reading. Each worker use own buffer, so memory consumption will be `read_buffer_size*workers_count`.
### max_files 
 `number` `default=16384` <br> <br> Max amount of opened files. If the limit is exceeded `file-d` will exit with fatal. Also check your file descriptors limit: `ulimit -n`.
### offsets_op 
 `string="continue|reset|tail"` default=`"continue"`

Offset operation which will be preformed when adding file as a job:
* `continue` – set offset as saved in offsets file.
* `reset` – set offset to the beginning of the file.
* `tail` – set offset to the end of the file.
> It is only used on initial scan of `watching_dir`. Files which will be catched up later during work always use `reset` operation.
### workers_count 
 `number` default=`16`

How much workers will be instantiated. Each worker:
* Read files (I/O bound)
* Decode events (CPU bound)
> It's recommended to set it to 4x-8x of CPU cores.

