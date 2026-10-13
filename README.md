# iot-things-v2

## Verifiering

```sh
gofmt -l .
go vet ./...
go test ./... -count=1
go build ./...
docker build -f deployments/Dockerfile .
```

### TODO

this will replace iot-things..
