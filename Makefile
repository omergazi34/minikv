.PHONY: build test race fuzz lint bench run

build:
	go build -o bin/minikv ./cmd/minikv
	go build -o bin/minikv-bench ./cmd/minikv-bench

test:
	go test ./...

race:
	go test -race ./...

fuzz:
	go test ./internal/resp -run '^$$' -fuzz=FuzzReadCommand -fuzztime=60s
	go test ./internal/store -run '^$$' -fuzz=FuzzMatch -fuzztime=30s

lint:
	golangci-lint run

run: build
	./bin/minikv -aof appendonly.aof

bench: build
	./bin/minikv -addr 127.0.0.1:6390 & pid=$$!; sleep 1; \
	./bin/minikv-bench -addr 127.0.0.1:6390 -c 50 -n 200000; \
	./bin/minikv-bench -addr 127.0.0.1:6390 -c 50 -n 1000000 -P 16 -t set,get; \
	kill $$pid
