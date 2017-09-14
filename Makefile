BIN_DIR := $(GOPATH)/bin
GOLINT  := $(BIN_DIR)/golint
GLIDE   := $(BIN_DIR)/glide
PGG     := $(BIN_DIR)/protoc-gen-go
PKGS    := $(shell go list ./... | grep -v vendor)
BINARY  := $(BIN_DIR)/bin/eventmaster

$(BINARY): deps $(wildcard **/*.go) proto vendor ui.go
	@go install -v github.com/ContextLogic/eventmaster/cmd/eventmaster

.PHONY: proto
proto: proto/eventmaster.pb.go

proto/eventmaster.pb.go: $(PGG) proto/eventmaster.proto
	protoc --plugin=${PGG} -I proto/ proto/eventmaster.proto --go_out=plugins=grpc:proto

.PHONY: test
test:
	@go test ${PGKS}

.PHONY: lint
lint: $(GOLINT)
	go vet .
	golint -set_exit_status .

$(GOLINT):
	go get -u github.com/golang/lint/golint

$(PGG):
	go get -u github.com/golang/protobuf/protoc-gen-go

.PHONY: run
run: $(BINARY)
	eventmaster -r

$(GLIDE):
	go install -v ./vendor/github.com/Masterminds/glide


.PHONY: deps
deps: vendor/gopkg.in
# here we randomly choose something I *know* glide will fetch
vendor/gopkg.in: $(GLIDE)
	glide install

ui.go: $(GBD) $(wildcard static/ui/**/*)
	go-bindata -prefix="static/" -o ui.go -pkg=eventmaster static/ui/...
