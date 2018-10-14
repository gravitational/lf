PROTOC_VER ?= 3.6.1
GOGO_PROTO_TAG ?= v1.1.1
PLATFORM := linux-x86_64
BUILDBOX_TAG := gravitationallf
BUILDDIR ?= build

# buildbox builds docker buildbox image used to compile binaries and generate GRPc stuff
.PHONY: buildbox
buildbox:
	docker build \
          --build-arg PROTOC_VER=$(PROTOC_VER) \
          --build-arg GOGO_PROTO_TAG=$(GOGO_PROTO_TAG) \
          --build-arg PLATFORM=$(PLATFORM) \
          -t $(BUILDBOX_TAG) .

# gen generates protobuf
.PHONY: gen
gen: buildbox
	docker run -v $(shell pwd):/go/src/github.com/gravitational/lf $(BUILDBOX_TAG) make -C /go/src/github.com/gravitational/lf buildbox-gen

# gen generates definitions from proto files,
# is run inside the buildbox
.PHONY: buildbox-gen
buildbox-gen:
# standard GRPC output
	echo $$PROTO_INCLUDE
	cd lf/walpb && protoc --gogofast_out=. -I=.:$$PROTO_INCLUDE *.proto

# sloccount returns a count of lines in go sources
.PHONY: sloccount
sloccount:
	find . -path ./vendor -prune -o -name "*.go" -print0 | xargs -0 wc -l

# bench runs benchmarks
.PHONY: bench
bench:
	go test ./lf -v -check.f=Benchmark -check.b -check.bmem

# cover runs tests with coverage on
.PHONY: cover
cover:
	go test ./lf -coverprofile=cover.out
	go tool cover -html=cover.out

# test runs tests with race detector and vetting
.PHONY: test
test:
	go test -race ./lf/...

.PHONY: lf
lf:
	mkdir -p $(BUILDDIR)
	go build -o $(BUILDDIR)/lf github.com/gravitational/lf/tool/lf


