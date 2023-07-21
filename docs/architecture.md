# Architecture 

Here is a bit simplified architecture of the **file.d** solution. 

![file.d](../static/file.d_arch.png)

What's going on here:

- **Input plugin** pulls data from external systems and pushes it next to the pipeline controller. Full list of input plugins available is [here](../plugin/input).
- The **pipeline controller** creates **streams** of the data and is in charge of converting data to event and subsequent routing.
- The **event pool** provides fast event instancing. 
- Events are processed by one or more **processors**. Every processor holds all **action plugins** from the configuration.
- Every moment the processor gets a stream of data, process 1 or more events and returns the stream to a **streamer** that is a pool of streams.
- Action plugins act on the events which meet particular criteria.
- Finally, the event goes to the **output plugins** and is dispatched to the external system.  

You can extend `file.d` by adding your own input, action, and output plugins. 

## Plugin endpoints
Every plugin can expose their own API via `PluginStaticInfo.Endpoints`.  
Plugin endpoints can be accessed via URL  
`/pipelines/<pipeline_name>/<plugin_index_in_config>/<plugin_endpoint>`.  
Input plugin has the index of zero, output plugin has the last index.  

#### `/info` and `/sample`
Actions also have the standard endpoints `/info` and `/sample`.  
If the action has `metric_name`, it will be collected and can be viewed via the `/info` endpoint.  
The `/sample` handler stores and shows an event before and after processing, so you can debug the action better.  
