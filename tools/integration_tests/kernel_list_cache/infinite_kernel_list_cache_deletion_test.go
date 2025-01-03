// Copyright 2024 Google LLC
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

package kernel_list_cache

import (
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/client"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/operations"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/setup"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/test_setup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"path"
	"testing"
)

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type infiniteKernelListCacheDeletionTest struct {
	flags []string
}

func (s *infiniteKernelListCacheDeletionTest) Setup(t *testing.T) {
	mountGCSFuseAndSetupTestDir(s.flags, ctx, storageClient, testDirName)
}

func (s *infiniteKernelListCacheDeletionTest) Teardown(t *testing.T) {
	setup.UnmountGCSFuse(rootDir)
}

func (s *infiniteKernelListCacheDeletionTest) TestKernelListCache_DeleteAndListDirectory(t *testing.T) {
	targetDir := path.Join(testDirPath, "explicit_dir")
	operations.CreateDirectory(targetDir, t)
	// Create test data
	f1 := operations.CreateFile(path.Join(targetDir, "file1.txt"), setup.FilePermission_0600, t)
	operations.CloseFile(f1)
	f2 := operations.CreateFile(path.Join(targetDir, "file2.txt"), setup.FilePermission_0600, t)
	operations.CloseFile(f2)

	err := os.RemoveAll(targetDir)
	assert.NoError(t, err)

	// Adding object to GCS to make sure to change the ReadDir() response.
	err = client.CreateObjectOnGCS(ctx, storageClient, path.Join(testDirName, "explicit_dir")+"/", "")
	require.NoError(t, err)
	client.CreateObjectInGCSTestDir(ctx, storageClient, testDirName, path.Join("explicit_dir", "file3.txt"), "", t)
	// Read will be served from GCS as removing the directory also deletes the cache.
	f, err := os.Open(targetDir)
	assert.NoError(t, err)
	names1, err := f.Readdirnames(-1)
	assert.NoError(t, err)
	require.Equal(t, 1, len(names1))
	assert.Equal(t, "file3.txt", names1[0])
	err = f.Close()
	assert.NoError(t, err)

	// 2nd RemoveAll call will also succeed.
	err = os.RemoveAll(targetDir)
	assert.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////
// Test Function (Runs once before all tests)
////////////////////////////////////////////////////////////////////////

func TestInfiniteKernelListCacheDeletionTest(t *testing.T) {
	ts := &infiniteKernelListCacheDeletionTest{}

	// Run tests for mounted directory if the flag is set.
	if setup.AreBothMountedDirectoryAndTestBucketFlagsSet() {
		test_setup.RunTests(t, ts)
		return
	}

	// Define flag set to run the tests.
	// Note: metadata cache is disabled to avoid cache consistency issue between
	// gcsfuse cache and kernel cache. As gcsfuse cache might hold the entry which
	// already became stale due to delete operation.
	// TODO: Replace metadata-cache-ttl-secs with something better
	flagsSet := [][]string{
		{"--kernel-list-cache-ttl-secs=-1", "--metadata-cache-ttl-secs=0"},
	}

	// Run tests.
	for _, flags := range flagsSet {
		ts.flags = flags
		log.Printf("Running tests with flags: %s", ts.flags)
		test_setup.RunTests(t, ts)
	}
}
