# /dev/null output
Provides API to test pipelines and other plugins.
> No config params

### API description
``SetOutFn(fn func(event *pipeline.Event))``

Sets up a hook to make sure test event have been successfully passed to output.


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*