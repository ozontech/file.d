# Input plugins

## fake
Provides API to test pipelines and other plugins.

[More details...](plugin/input/fake/README.md)
## file
Watches for files in the provided directory and reads them line by line.

Each line should contain only one event. It also correctly handles rotations (rename/truncate) and symlinks.
From time to time it instantly releases and reopens descriptors of completely processed files.
Such behaviour allows files to be deleted by third party software even though `file.d` is still working (in this case reopen will fail).
Watcher is trying to use file system events detect file creation and updates.
But update events don't work with symlinks, so watcher also periodically manually `fstat` all tracking files to detect changes.

> ⚠ It supports commitment mechanism. But at least once delivery guarantees only if files aren't being truncated.
> However, `file.d` correctly handles file truncation, there is a little chance of data loss.
> It isn't an `file.d` issue. Data may have been written just before file truncation. In this case, you may late to read some events.
> If you care about delivery, you should also know that `logrotate` manual clearly states that copy/truncate may cause data loss even on a rotating stage.
> So use copy/truncate or similar actions only if your data isn't very important.


**Reading docker container log files:**
```yaml
pipelines:
  example_docker_pipeline:
	input:
		type: file
		watching_dir: /var/lib/docker/containers
		offsets_file: /data/offsets.yaml
		filename_pattern: "*-json.log"
		persistence_mode: async
```

[More details...](plugin/input/file/README.md)
## http
Reads events from HTTP requests with body delimited by a new line.

Also it emulates some protocols to allow receive events from wide range of software which use HTTP to transmit data.
E.g. `file.d` may pretends to be Elasticsearch allows clients to send events using Elasticsearch protocol.
So you can use Elasticsearch filebeat output plugin to send data to `file.d`.

> ⚠ Currently event commitment mechanism isn't implemented for this plugin.
> Plugin answers with HTTP code `OK 200` right after it have read all the request body.
> It doesn't wait until events will be committed.

[More details...](plugin/input/http/README.md)
## kafka
Reads events from multiple kafka topics using `sarama` library.
> It guaranties at least once delivery due to commitment mechanism.

[More details...](plugin/input/kafka/README.md)
<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*