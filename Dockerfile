FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o bloodraven ./cmd/bloodraven

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /app/bloodraven /bloodraven
ENTRYPOINT ["/bloodraven"]
