FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /trifolium ./cmd/trifolium

FROM alpine:3.24.4
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 trifolium && mkdir /data && chown trifolium /data
COPY --from=build /trifolium /usr/local/bin/trifolium
COPY LICENSE /usr/share/licenses/trifolium/LICENSE
USER trifolium
WORKDIR /data
VOLUME ["/data"]
ENTRYPOINT ["trifolium"]
