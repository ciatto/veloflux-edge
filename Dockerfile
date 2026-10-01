FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod ./
COPY main.go ./
COPY main_test.go ./
RUN go test ./... -count=1
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/veloflux-edge .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates \
    && addgroup -g 10001 veloflux \
    && adduser -D -u 10001 -G veloflux veloflux
COPY --from=builder /out/veloflux-edge /veloflux-edge
USER veloflux:veloflux
ENV PORT=8080
CMD ["/veloflux-edge"]
