FROM golang:alpine3.14 as BUILDER
WORKDIR /opt/rmb
COPY . .
WORKDIR /opt/rmb/cmds/msgbusd
RUN go build .

FROM alpine:3.13.5
COPY --from=BUILDER /opt/rmb/cmds/msgbusd/msgbusd /bin/
ENTRYPOINT [ "/bin/msgbusd" ]
