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
	"testing"

	. "github.com/jacobsa/ogletest"
)

func TestMemoryBuffer(t *testing.T) { RunTests(t) }

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type MemoryBufferTest struct {
	mb *MemoryBuffer
}

var _ SetUpInterface = &MemoryBufferTest{}
var _ TearDownInterface = &MemoryBufferTest{}

func init() { RegisterTestSuite(&MemoryBufferTest{}) }

func (t *MemoryBufferTest) SetUp(ti *TestInfo) {
	t.mb = &MemoryBuffer{}
}

func (t *MemoryBufferTest) TearDown() {}

// //////////////////////////////////////////////////////////////////////
// Tests
// //////////////////////////////////////////////////////////////////////

func (t *MemoryBufferTest) TestCreateInMemoryBuffer() {
	bufferSize := 10

	t.mb.Create(bufferSize)

	AssertEq(bufferSize*1024*1024, t.mb.chunkSize)
	AssertEq(2*bufferSize*1024*1024, t.mb.buffer.Cap())
	AssertEq(0, t.mb.buffer.Len())
}
