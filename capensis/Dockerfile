FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /capensis ./cmd/capensis

FROM alpine:3.24.4
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 capensis && mkdir /data && chown capensis /data
COPY --from=build /capensis /usr/local/bin/capensis
COPY LICENSE /usr/share/licenses/capensis/LICENSE
USER capensis
WORKDIR /data
VOLUME ["/data"]
ENTRYPOINT ["capensis"]
