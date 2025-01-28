// Copyright 2025 Google LLC
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

package streaming_writes

import (
	"log"
	"os"
	"testing"

	emulator_tests "github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/emulator_tests/util"
	. "github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/client"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/operations"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/setup"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// //////////////////////////////////////////////////////////////////////
// Boilerplate
// //////////////////////////////////////////////////////////////////////

type uploadFailureTestSuite struct {
	suite.Suite
	flags []string
}

////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////

func (t *uploadFailureTestSuite) SetupSuite() {
	log.Print("Inside Setup Suite...[uploadFailureTestSuite]")
	log.Printf("Test log: %s\n", setup.LogFile())
	configPath := "/usr/local/google/home/mohitkyadav/gcsfuse/tools/integration_tests/emulator_tests/proxy_server/configs/upload_failure_return400_on_third_chunk_upload.yaml"
	emulator_tests.StartProxyServer(configPath)

}

func (t *uploadFailureTestSuite) TearDownSuite() {
	log.Print("Inside TearDown Suite...[uploadFailureTestSuite]")
	setup.UnmountGCSFuse(rootDir)
	assert.NoError(t.T(), emulator_tests.KillProxyServerProcess(port))
}

func (t *uploadFailureTestSuite) TestStreamingWritesFirstChunkUploadFails() {
	t.flags = []string{"--log-severity=TRACE", "--enable-streaming-writes=true", "--write-block-size-mb=2", "--write-max-blocks-per-file=2", "--custom-endpoint=" + proxyEndpoint}
	log.Printf("Running tests with flags: %v", t.flags)
	setup.MountGCSFuseWithGivenMountFunc(t.flags, mountFunc)
	testDirPath = setup.SetupTestDirectory(testDirName)
	// Create a local file.
	filePath, fh1 := CreateLocalFileInTestDir(ctx, storageClient, testDirPath, FileName1, t.T())
	data, err := operations.GenerateRandomData(4 * 1024 * 1024)
	if err != nil {
		t.T().Fatalf("Error in generating data: %v", err)
	}

	// Write 4 MB data to file succeeds.
	operations.WriteAt(string(data[:]), 0, fh1, t.T())

	fh2, err := os.OpenFile(filePath, os.O_WRONLY, operations.FilePermission_0600)

	if err != nil {
		t.T().Fatalf("Error in opening file: %v", err)
	}
	// Write next 4 MB data to file fails due to 3rd chunk upload permanently fails.
	_, err = fh2.WriteAt(data[:], 4*1024*1024)

	
	// Close the file and validate that the file is created on GCS.
	CloseFileAndValidateContentFromGCS(ctx, storageClient, fh1, testDirName,
		FileName1, string(data[:]), t.T())
}

func TestUploadFailureTestSuite(t *testing.T) {
	suite.Run(t, new(uploadFailureTestSuite))
}
