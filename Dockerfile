# syntax=docker/dockerfile:1
# Build: docker build -t minikv .
# Run:   docker run -p 6380:6380 -v minikv-data:/data minikv

FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /minikv ./cmd/minikv
# The data directory must belong to the unprivileged runtime user (65532 in distroless), or a
# fresh named volume would be root-owned and the AOF could not be created.
RUN mkdir -p /out/data

# A static binary needs nothing else: no shell, no libc, nothing to patch.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /minikv /minikv
COPY --from=build --chown=65532:65532 /out/data /data
VOLUME /data
EXPOSE 6380
ENTRYPOINT ["/minikv", "-addr", ":6380", "-aof", "/data/appendonly.aof"]
