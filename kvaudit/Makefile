all: editor
	go vet
	go install
	go clean
	make todo

editor:
	go fmt
	go test -i
	go test
	go build

todo:
	@grep -n ^[[:space:]]*_[[:space:]]*=[[:space:]][[:alnum:]] *.go || true
	@grep -n TODO *.go || true
	@grep -n FIXME *.go || true
	@grep -n BUG *.go || true

clean:
	go clean

gocov:
	gocov test $(COV) | gocov-html > cov.html
