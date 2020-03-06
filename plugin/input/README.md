# Input plugins

## fake
It provides an API to test pipelines and other plugins.

[More details...](plugin/input/fake/README.md)
## file
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
Reads events from HTTP requests with the body delimited by a new line.

Also, it emulates some protocols to allow receiving events from a wide range of software that use HTTP to transmit data.
E.g. `file.d` may pretend to be Elasticsearch allows clients to send events using Elasticsearch protocol.
So you can use Elasticsearch filebeat output plugin to send data to `file.d`.

> ⚠ Currently event commitment mechanism isn't implemented for this plugin.
> Plugin answers with HTTP code `OK 200` right after it has read all the request body.
> It doesn't wait until events are committed.

[More details...](plugin/input/http/README.md)
## kafka
It reads events from multiple Kafka topics using `sarama` library.
> It guarantees at "at least once delivery" due to the commitment mechanism.

[More details...](plugin/input/kafka/README.md)
<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*