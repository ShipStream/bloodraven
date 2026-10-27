FROM golang:1.27-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o ledger .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /app/ledger /ledger
ENTRYPOINT ["/ledger"]
