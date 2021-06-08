FROM golang:1.16.5

RUN apt-get update -q
RUN apt-get install -y protobuf-compiler

COPY . /go/src/github.com/wish/eventmaster
WORKDIR /go/src/github.com/wish/eventmaster

RUN make

EXPOSE 50052
CMD ["eventmaster"]