# Copyright 2014 The kv Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

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
