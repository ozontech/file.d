## Testing
In order to start tests, use standard `go test` command:  
```go
go test ./...
```

If you want to launch only the fast tests, consider using `-short` flag.  
In order to start end-to-end test you should add `e2e` tag:  
```go
go test -tags=e2e ./...
```
