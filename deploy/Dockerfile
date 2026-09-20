# syntax=docker/dockerfile:1

# Which binary to build: ratelimiterd (default) or demoapp.
ARG CMD=ratelimiterd

FROM golang:1.26-alpine AS build
ARG CMD
WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module
# cache on every build.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

# Static binary, no shell, no package manager: nothing to exploit and nothing
# to patch.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
COPY configs/config.yaml /etc/ratelimiter/config.yaml
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app"]
CMD ["-config", "/etc/ratelimiter/config.yaml"]
