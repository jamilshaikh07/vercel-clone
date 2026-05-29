# Multi-stage build: tiny static binary in a distroless runtime.
# Kaniko handles both stages rootless on the cluster.
FROM golang:1.22-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -ldflags="-s -w" -o /out/control-plane .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/control-plane /control-plane
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/control-plane"]
