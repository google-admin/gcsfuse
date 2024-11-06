// Copyright 2024 Google Inc. All Rights Reserved.
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

package benchmarking

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/benchmark_setup"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/operations"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/setup"
)

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type benchmarkStatTest struct{}

func (s *benchmarkStatTest) SetupB(t *testing.B) {
	testDirPath = setup.SetupTestDirectory(testDirName)
}

func (s *benchmarkStatTest) TeardownB(t *testing.B) {}

////////////////////////////////////////////////////////////////////////
// Test scenarios
////////////////////////////////////////////////////////////////////////

func (s *benchmarkStatTest) Benchmark_Stat(b *testing.B) {
	fmt.Println("Value of ", b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := operations.StatFile(path.Join(testDirPath, "a.txt"))
		if err != nil {
			b.Errorf("testing error: %v", err)
		}
		// Code to be benchmarked goes here
		// For example:
		x := 10
		y := 20
		z := x + y
		time.Sleep(1 * time.Second)
		fmt.Println("Addition: ", z)
	}
}

////////////////////////////////////////////////////////////////////////
// Test Function (Runs once before all tests)
////////////////////////////////////////////////////////////////////////

func Benchmark_Stat(t *testing.B) {
	ts := &benchmarkStatTest{}
	benchmark_setup.RunBenchmarks(t, ts)
}
