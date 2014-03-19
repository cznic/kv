.PHONY: all editor todo clean nuke

grep=--include=*.go --include=*.run --include=*.y

all: editor
	go vet
	go install
	go clean
	make todo

clean:
	go clean

editor:
	go fmt
	go test -i
	go test
	go build

nuke:
	go clean -i

todo:
	@grep -nr $(grep) ^[[:space:]]*_[[:space:]]*=[[:space:]][[:alpha:]][[:alnum:]]* * || true
	@grep -nrw $(grep) TODO * || true
	@grep -nrw $(grep) BUG * || true
	@grep -nrw $(grep) println * || true
