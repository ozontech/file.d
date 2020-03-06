# /dev/null output
It provides an API to test pipelines and other plugins.
> No config params

### API description
``SetOutFn(fn func(event *pipeline.Event))``

It sets up a hook to make sure the test event passes successfully to output.


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*