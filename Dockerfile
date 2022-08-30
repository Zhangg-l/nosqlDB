FROM golang:latest
 
WORKDIR $GOPATH/src/test

COPY rosedb-server  $GOPATH/src/test

EXPOSE 11000

ENTRYPOINT ["./rosedb-server"]

