# Builds the Helix image ghcr.io/helix-nextgen-cellular-core/udm:v1.0.0
# the same way it was built and verified:
#   docker build -t udm:local .
FROM golang:1.18.3-stretch AS builder
WORKDIR /go/src/udm
COPY . .
RUN go mod vendor \
 && make all && mkdir -p /out/free5gc/udm && cp bin/* /out/free5gc/udm/

FROM alpine:3.16
LABEL org.opencontainers.image.source="https://github.com/Helix-NextGen-Cellular-Core/udm" \
      org.opencontainers.image.licenses="Apache-2.0"
RUN apk update && apk add -U vim strace net-tools curl netcat-openbsd bind-tools
COPY --from=builder /out/ /
WORKDIR /free5gc/udm
