FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o dns-webhook .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /app/dns-webhook /dns-webhook
ENTRYPOINT ["/dns-webhook"]
