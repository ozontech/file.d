# Contributing

This document explains the process of contributing to the **file.d** project.

`file.d` is an open-source project. Contributing is very welcome! For now, it's free of rules.

## Testing
In order to start tests, use standard `go test` command:  
```go
go test ./...
```

If you want to launch only fast ones, consider using `-short` flag.  
In order to start end-to-end test you should add `e2e` tag:  
```go
go test -tags=e2e ./...
```
