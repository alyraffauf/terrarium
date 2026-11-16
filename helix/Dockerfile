FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /helix ./cmd/helix

FROM alpine:3.24.4
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 helix && mkdir /data && chown helix /data
COPY --from=build /helix /usr/local/bin/helix
COPY LICENSE /usr/share/licenses/helix/LICENSE
USER helix
WORKDIR /data
VOLUME ["/data"]
ENTRYPOINT ["helix"]
