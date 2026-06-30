FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/server    ./cmd/server  && \
    CGO_ENABLED=0 GOOS=linux go build -o /bin/worker    ./cmd/worker  && \
    CGO_ENABLED=0 GOOS=linux go build -o /bin/dev       ./cmd/dev

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /bin/server /bin/server
COPY --from=builder /bin/worker /bin/worker
COPY --from=builder /bin/dev    /bin/dev
ENTRYPOINT ["/bin/server"]
