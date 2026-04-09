FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
ARG TARGETOS TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o bloodraven ./cmd/bloodraven
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o sidecar ./cmd/sidecar

FROM gcr.io/distroless/static-debian12:nonroot AS bloodraven
COPY --from=builder /app/bloodraven /bloodraven
ENTRYPOINT ["/bloodraven"]

FROM gcr.io/distroless/static-debian12:nonroot AS sidecar
COPY --from=builder /app/sidecar /sidecar
ENTRYPOINT ["/sidecar"]
