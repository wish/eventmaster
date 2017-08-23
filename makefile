SHELL:=/bin/bash -O extglob

protobuf:
	protoc --plugin=$(GOPATH)/bin/protoc-gen-go -I proto/ proto/eventmaster.proto --go_out=plugins=grpc:proto

run:
	go run src/eventmaster/!(*_test).go -p -r

test:
	go test src/eventmaster/*.go
