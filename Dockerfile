FROM golang:1.19.3 AS base

FROM base AS code
ARG GOARCH=amd64
WORKDIR /src
ADD . .

FROM code AS build
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod \
  GOOS=linux GOARCH=${GOARCH} CGO_ENABLED=0 go build -o s3-plugin -v -ldflags '-extldflags "-static"'

FROM code AS test

FROM base AS jaeger-grpc-integration
ARG GOARCH=amd64
RUN git clone --depth=1 --single-branch --branch=v1.32.0 https://github.com/jaegertracing/jaeger.git /jaeger
WORKDIR /jaeger
COPY --from=build /src/s3-plugin /go/bin

FROM jaegertracing/all-in-one:1.39.0 AS jaeger-test
COPY --from=build /src/s3-plugin /go/bin

FROM alpine:3.16.3

COPY --from=build /src/s3-plugin /jaeger-s3

# The /plugin is used by the jaeger-operator https://github.com/jaegertracing/jaeger-operator/pull/1517
CMD ["cp", "/jaeger-s3", "/plugin/jaeger-s3"]
