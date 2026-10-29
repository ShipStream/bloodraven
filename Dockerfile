# deps has no target ARGs, so BuildKit shares it across platforms and targets.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS deps
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# One compile stage per target: `--target bloodraven` must not compile the sidecar.
FROM deps AS build-bloodraven
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o bloodraven ./cmd/bloodraven

FROM deps AS build-sidecar
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o sidecar ./cmd/sidecar

# Runtime stages contain only COPY/ENTRYPOINT. CI builds linux/arm64 without
# QEMU; adding a RUN here would need emulation again (scan.yml, release.yml).
FROM gcr.io/distroless/static-debian12:nonroot AS bloodraven
COPY --from=build-bloodraven /app/bloodraven /bloodraven
ENTRYPOINT ["/bloodraven"]

FROM gcr.io/distroless/static-debian12:nonroot AS sidecar
COPY --from=build-sidecar /app/sidecar /sidecar
ENTRYPOINT ["/sidecar"]
