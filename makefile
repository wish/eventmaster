SHELL:=/bin/bash -O extglob

protobuf:
	protoc --plugin=$(GOPATH)/bin/protoc-gen-go -I eventmaster/ eventmaster/eventmaster.proto --go_out=plugins=grpc:eventmaster

run:
	go run src/eventmaster/!(*_test).go -p -r

test:
	go test src/eventmaster/*.go
