FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o counter-app .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /app/counter-app /counter-app
ENTRYPOINT ["/counter-app"]
