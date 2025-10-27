FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY . .

RUN go build -o /build ./cmd/main.go

FROM alpine:latest

RUN apk add --no-cache iptables

COPY --from=builder /build /build

ENTRYPOINT ["./build"]