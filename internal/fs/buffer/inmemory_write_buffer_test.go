// Copyright 2023 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buffer

import (
	"bytes"
	"testing"

	. "github.com/jacobsa/ogletest"
)

func TestMemoryBuffer(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type MemoryBufferTest struct {
	mb *InMemoryWriteBuffer
}

var _ SetUpInterface = &MemoryBufferTest{}
var _ TearDownInterface = &MemoryBufferTest{}

func init() { RegisterTestSuite(&MemoryBufferTest{}) }

func (t *MemoryBufferTest) SetUp(ti *TestInfo) {
	t.mb = &InMemoryWriteBuffer{}
}

func (t *MemoryBufferTest) TearDown() {}

// //////////////////////////////////////////////////////////////////////
// Tests
// //////////////////////////////////////////////////////////////////////

func (t *MemoryBufferTest) TestCreateEmptyInMemoryBuffer() {
	t.mb = CreateInMemoryWriteBuffer()

	AssertEq(nil, t.mb.buffer)
	AssertEq(0, ChunkSize)
}

func (t *MemoryBufferTest) TestInitializeInMemoryBuffer() {
	sizeInMB := 1
	t.mb = CreateInMemoryWriteBuffer()

	t.mb.InitializeBuffer(sizeInMB)

	AssertEq(sizeInMB*MiB, ChunkSize)
	AssertEq(2*sizeInMB*MiB, cap(t.mb.buffer))
	AssertEq(0, len(t.mb.buffer))
}

func (t *MemoryBufferTest) TestSingleWriteToInMemoryBuffer() {
	// Allocate a buffer
	t.TestInitializeInMemoryBuffer()
	data := []byte("Taco")

	// Write to buffer
	err := t.mb.WriteAt(data, 0)

	AssertEq(nil, err)
	AssertEq(t.mb.fileSize, 4)
	AssertEq(true, bytes.Equal(data, t.mb.buffer[0:t.mb.fileSize]))
}

func (t *MemoryBufferTest) TestMultipleSequentialWritesToInMemoryBuffer() {
	// Allocate a buffer
	t.TestInitializeInMemoryBuffer()

	// Write to buffer
	err := t.mb.WriteAt([]byte("Taco"), 0)
	AssertEq(nil, err)
	err = t.mb.WriteAt([]byte("Burrito"), 4)
	AssertEq(nil, err)
	err = t.mb.WriteAt([]byte("Pizza"), 11)
	AssertEq(nil, err)

	AssertEq(t.mb.fileSize, 16)
	AssertEq(true, bytes.Equal([]byte("TacoBurritoPizza"), t.mb.buffer[0:t.mb.fileSize]))
}

func (t *MemoryBufferTest) TestMultipleRandomWritesToInMemoryBuffer() {
	// Allocate a buffer
	t.TestInitializeInMemoryBuffer()

	// Write to buffer
	err := t.mb.WriteAt([]byte("Taco"), 0)
	AssertEq(nil, err)
	err = t.mb.WriteAt([]byte("Burrito"), 2)
	AssertEq(nil, err)
	err = t.mb.WriteAt([]byte("Pizza"), 7)
	AssertEq(nil, err)

	AssertEq(t.mb.fileSize, 12)
	AssertEq(true, bytes.Equal([]byte("TaBurriPizza"), t.mb.buffer[0:t.mb.fileSize]))
}

func (t *MemoryBufferTest) TestWriteJustBeforeChunkSizeOffset() {
	// Allocate a buffer
	t.TestInitializeInMemoryBuffer()

	// Write to buffer
	err := t.mb.WriteAt([]byte("Taco"), MiB-1)
	AssertEq(nil, err)

	AssertEq(t.mb.fileSize, MiB+3)
	AssertEq(true, bytes.Equal([]byte("Taco"), t.mb.buffer[MiB-1:MiB+3]))
}

func (t *MemoryBufferTest) TestWriteAtChunkSizeOffset() {
	// Allocate a buffer
	t.TestInitializeInMemoryBuffer()

	// Write to buffer
	err := t.mb.WriteAt([]byte("Taco"), MiB)
	AssertEq(nil, err)

	AssertEq(t.mb.fileSize, MiB+4)
	AssertEq(true, bytes.Equal([]byte("Taco"), t.mb.buffer[MiB:MiB+4]))
}

func (t *MemoryBufferTest) TestWriteJustAfterChunkSizeOffset() {
	// Allocate a buffer
	t.TestInitializeInMemoryBuffer()

	// Write to buffer
	err := t.mb.WriteAt([]byte("Taco"), MiB+1)

	AssertNe(nil, err)
	AssertEq(NonSequentialWriteError, err.Error())
	AssertEq(t.mb.fileSize, 0)
}
