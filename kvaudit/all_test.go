// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"path/filepath"
	"testing"
)

const testdata = "_testdata"

func TestBad(t *testing.T) {
	if err := main0(filepath.Join(testdata, "bad.db"), 0, null, false); err == nil {
		t.Fatal("unexpected success")
	}
}

func TestGood(t *testing.T) {
	if err := main0(filepath.Join(testdata, "good.db"), 0, null, false); err != nil {
		t.Fatal(err)
	}
}
