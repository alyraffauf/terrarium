FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /cordata ./cmd/cordata

FROM alpine:3.24.4
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 cordata
COPY --from=build /cordata /usr/local/bin/cordata
COPY LICENSE /usr/share/licenses/cordata/LICENSE
USER cordata
ENTRYPOINT ["cordata"]
