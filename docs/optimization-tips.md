# Optimization tips

## CPU
* Limit regular expressions usage if you care about CPU load.
* File input plugin must have at least X files to process data efficiently. Where X is a number of CPU cores.
* Plugin parameters such as `worker_count`, `batch_size` may have a significant impact on throughput. Tune them for your system and needs.
* Pipeline parameter `capacity` is also highly tunable. Bigger sizes increase performance but increase memory usage too.

## RAM
* Memory usage highly depends on maximum event size. In the examples below, we assume that `event_size` is a maximum event size that can get into the pipeline.       
* Main RAM consumers are output buffers. Rough estimation is worker_count×batch_size×event_size. For worker_count=16, batch_size=256, event_size=64KB output buffers will take 16×256×64KB=256MB.
* Next significant RAM consumer an event pool. A rough estimation is capacity×event_size. So if you have a pipeline with capacity=1024 and event_size=64KB, then the event pool size will be 1024×64KB=64MB.
* For file input plugin buffers takes worker_count×read_buffer_size of RAM which is 2MB, if worker_count=16 and read_buffer_size=128KB.
* Total estimation of RAM usage is input_buffers+event_pool+output_buffers, which is 64MB+256MB+2MB=322MB for examples above.

