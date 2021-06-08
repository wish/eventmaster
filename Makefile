GOPATH  := $(shell go env GOPATH)
BIN_DIR := $(GOPATH)/bin
GOLINT  := $(BIN_DIR)/golint
PGG     := $(BIN_DIR)/protoc-gen-go
GBD     := $(BIN_DIR)/go-bindata
PKGS    := $(shell go list ./... | grep -v vendor | grep -v ui$ | grep -v templates$ )
BINARY  := $(BIN_DIR)/bin/eventmaster

VERSION := $(shell git describe --tags 2> /dev/null || echo "unreleased")
V_DIRTY := $(shell git describe --exact-match HEAD 2> /dev/null > /dev/null || echo "-unreleased")
GIT     := $(shell git rev-parse --short HEAD)
DIRTY   := $(shell git diff-index --quiet HEAD 2> /dev/null > /dev/null || echo "-dirty")


$(BINARY): $(wildcard **/*.go) proto ui/ui.go templates/templates.go deps
	@go install -v -ldflags \
		"-X github.com/wish/eventmaster.Version=$(VERSION)$(V_DIRTY) \
		 -X github.com/wish/eventmaster.Git=$(GIT)$(DIRTY)" \
		github.com/wish/eventmaster/cmd/...

.PHONY: proto
proto: proto/eventmaster.pb.go 

proto/eventmaster.pb.go: $(PGG) proto/eventmaster.proto
	protoc --plugin=${PGG} -I proto/ proto/eventmaster.proto --go_out=plugins=grpc:proto
	cp proto/github.com/wish/eventmaster/eventmaster.pb.go proto/

.PHONY: test
test: proto/eventmaster.pb.go ui/ui.go templates/templates.go
	@go test -cover ${PKGS}

.PHONY: lint
lint: $(GOLINT)
	@go vet ${PKGS}
	@golint -set_exit_status ${PKGS}

$(GOLINT):
	go get -u github.com/golang/lint/golint@1.11

$(PGG):
	go get -u github.com/golang/protobuf/protoc-gen-go@v1.5.2

$(GBD):
	go get -u github.com/jteeuwen/go-bindata/go-bindata@master

.PHONY: deps
deps: vendor
vendor:
	go mod vendor

ui:
	@mkdir ui

ui/ui.go: $(GBD) $(wildcard static/ui/**/*) ui
	$(GBD) -prefix="static/" -o ui/ui.go -pkg=ui static/ui/...

templates:
	@mkdir templates

templates/templates.go: $(GBD) $(wildcard static/templates/*) templates
	$(GBD) -prefix="static/" -o templates/templates.go -pkg=templates static/templates/...

.PHONY: coverage
coverage: 
	@go test -coverprofile=/tmp/cover github.com/wish/eventmaster 
	@go tool cover -html=/tmp/cover -o coverage.html
	@rm /tmp/cover
