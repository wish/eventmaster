BIN_DIR := $(GOPATH)/bin
GOLINT  := $(BIN_DIR)/golint
GLIDE   := $(BIN_DIR)/glide
PGG     := $(BIN_DIR)/protoc-gen-go
GBD     := $(BIN_DIR)/go-bindata
PKGS    := $(shell go list ./... | grep -v vendor)
BINARY  := $(BIN_DIR)/bin/eventmaster

$(BINARY): deps $(wildcard **/*.go) proto vendor ui.go templates/templates.go
	@go install github.com/ContextLogic/eventmaster/cmd/eventmaster

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

$(GBD):
	go install ./vendor/github.com/jteeuwen/go-bindata/go-bindata

.PHONY: run
run: $(BINARY)
	eventmaster -r

$(GLIDE):
	go install ./vendor/github.com/Masterminds/glide


.PHONY: deps
deps: vendor/gopkg.in
# here we randomly choose something I *know* glide will fetch
vendor/gopkg.in: $(GLIDE)
	glide --quiet install

ui.go: $(GBD) $(wildcard static/ui/**/*)
	go-bindata -prefix="static/" -o ui.go -pkg=eventmaster static/ui/...

templates:
	@mkdir templates

templates/templates.go: $(GBD) $(wildcard static/templates/*) templates
	go-bindata -prefix="static/" -o templates/templates.go -pkg=templates static/templates/...
