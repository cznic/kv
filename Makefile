.PHONY: all editor todo clean nuke

all: editor
	go build
	go vet
	go install
	make todo

editor:
	go fmt
	go test -i
	go test

todo:
	@grep -n ^[[:space:]]*_[[:space:]]*=[[:space:]][[:alnum:]] *.go || true
	@grep -n TODO *.go || true
	@grep -n FIXME *.go || true
	@grep -n BUG *.go || true

clean:
	go clean
	rm -f *~ _testdata/temp*

nuke: clean
	go clean -i
