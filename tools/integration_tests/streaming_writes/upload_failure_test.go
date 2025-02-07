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
}

func (t *uploadFailureTestSuite) SetupTest() {
	err := emulator_tests.KillProxyServerProcess(port)
	log.Printf("Trying to stop the proxy server: [%v]", err)
	configPath := "proxy_server_configs/upload_failure_return412_on_second_chunk_upload.yaml"
	emulator_tests.StartProxyServer(configPath)
}

func (t *uploadFailureTestSuite) TearDownTest() {
	log.Print("Inside TearDownTest")
	setup.UnmountGCSFuse(rootDir)
	assert.NoError(t.T(), emulator_tests.KillProxyServerProcess(port))
}

func (t *uploadFailureTestSuite) TestStreamingWritesSecondChunkUploadFailure() {
	t.flags = []string{"--log-severity=TRACE", "--enable-streaming-writes=true", "--write-block-size-mb=1", "--write-max-blocks-per-file=1", "--custom-endpoint=" + proxyEndpoint}
	log.Printf("Running tests with flags: %v", t.flags)
	setup.MountGCSFuseWithGivenMountFunc(t.flags, mountFunc)
	testDirPath = setup.SetupTestDirectory(testDirName)
	// Create a local file.
	filePath, fh1 := CreateLocalFileInTestDir(ctx, storageClient, testDirPath, FileName1, t.T())
	// Generate 5 MB random data.
	data, err := operations.GenerateRandomData(5 * operations.MiB)
	if err != nil {
		t.T().Fatalf("Error in generating data: %v", err)
	}
	// Write first 3 MB (say A,B,C) block to file succeeds.
	// Fuse:[C] -> Go-SDK:[B]-> GCS[A]
	_, err = fh1.WriteAt(data[:3*operations.MiB], 0)
	assert.NoError(t.T(), err)
	// Write 4th 1MB (D) ensures the chunk (B) is uploaded to have enough space for C, D and error is propagated after the 4th 1MB is written.
	// Fuse:[D] -> Go-SDK:[C] -> GCS[A, B -> upload fails]
	_, err = fh1.WriteAt(data[3*operations.MiB:4*operations.MiB], 3*operations.MiB)

	// Write 5th 1MB results in errors as it sees the error propagated via B upload failure.
	_, err = fh1.WriteAt(data[4*operations.MiB:5*operations.MiB], 4*operations.MiB)

	assert.Error(t.T(), err)
	// Opening new file handle succeeds.
	fh2 := operations.OpenFile(filePath, t.T())
	// Writes with fh2 also fails.
	_, err = fh2.WriteAt(data[4*operations.MiB:5*operations.MiB], 4*operations.MiB)
	assert.Error(t.T(), err)
	// Closing all file handles to reinitialize bwh.
	operations.CloseFileShouldThrowError(fh2, t.T())

	operations.CloseFileShouldThrowError(fh1, t.T())
	// Verify that Object is not found on GCS.
	ValidateObjectNotFoundErrOnGCS(ctx, storageClient, testDirName, FileName1, t.T())
	// Opening new file handle and writing to file succeeds.
	log.Println("============Opening new file handle now after closing all")
	fh3 := operations.OpenFile(filePath, t.T())
	_, err = fh3.WriteAt(data[:], 0)
	assert.NoError(t.T(), err)
	// Validate object content found on GCS.
	ValidateObjectContentsFromGCS(ctx, storageClient, testDirName, FileName1, string(data), t.T())
}

// func (t *uploadFailureTestSuite) TestStreamingWritesTruncateToSmallerWhenSecondChunkUploadFailure() {
// 	t.flags = []string{"--log-severity=TRACE", "--enable-streaming-writes=true", "--write-block-size-mb=1", "--write-max-blocks-per-file=1", "--custom-endpoint=" + proxyEndpoint}
// 	log.Printf("Running tests with flags: %v", t.flags)
// 	setup.MountGCSFuseWithGivenMountFunc(t.flags, mountFunc)
// 	testDirPath = setup.SetupTestDirectory(testDirName)
// 	// Create a local file.
// 	filePath, fh1 := CreateLocalFileInTestDir(ctx, storageClient, testDirPath, FileName1, t.T())
// 	// Generate 5 MB random data.
// 	data, err := operations.GenerateRandomData(5 * operations.MiB)
// 	if err != nil {
// 		t.T().Fatalf("Error in generating data: %v", err)
// 	}
// 	// Write first 3 MB (say A,B,C) block to file succeeds.
// 	// Fuse:[C] -> Go-SDK:[B]-> GCS[A]
// 	_, err = fh1.WriteAt(data[:3*operations.MiB], 0)
// 	assert.NoError(t.T(), err)

// 	// Write 4th 1MB (D) ensures the chunk (B) is uploaded to have enough space for C, D and error is propagated for 4th 1MB (D) Write.
// 	// Fuse:[D -> write fails] -> Go-SDK:[C] -> GCS[A, B -> upload fails]
// 	_, err = fh1.WriteAt(data[3*operations.MiB:4*operations.MiB], 3*operations.MiB)

// 	assert.Error(t.T(), err)
// 	// Truncate to smaller size fails.
// 	err = fh1.Truncate(1 * operations.MiB)
// 	assert.Error(t.T(), err)
// 	// Opening new file handle succeeds.
// 	fh2 := operations.OpenFile(filePath, t.T())
// 	// Write still fails.
// 	_, err = fh2.WriteAt(data[3*operations.MiB:4*operations.MiB], 3*operations.MiB)
// 	assert.Error(t.T(), err)
// 	// Closing all file handles to reinitialize bwh.
// 	operations.CloseFileShouldThrowError(fh2, t.T())
// 	operations.CloseFileShouldThrowError(fh1, t.T())
// 	// Verify that Object is not found on GCS.
// 	ValidateObjectNotFoundErrOnGCS(ctx, storageClient, testDirName, FileName1, t.T())
// 	fh3 := operations.OpenFile(filePath, t.T())
// 	_, err = fh3.WriteAt(data[:], 0)
// 	assert.NoError(t.T(), err)
// 	// Validate object content found on GCS.
// 	ValidateObjectContentsFromGCS(ctx, storageClient, testDirName, FileName1, string(data), t.T())
// }

// func (t *uploadFailureTestSuite) TestStreamingWritesTruncateBiggerWithUploadError() {
// 	t.flags = []string{"--log-severity=TRACE", "--enable-streaming-writes=true", "--write-block-size-mb=1", "--write-max-blocks-per-file=1", "--custom-endpoint=" + proxyEndpoint}
// 	log.Printf("Running tests with flags: %v", t.flags)
// 	setup.MountGCSFuseWithGivenMountFunc(t.flags, mountFunc)
// 	testDirPath = setup.SetupTestDirectory(testDirName)
// 	// Create a local file.
// 	filePath, fh1 := CreateLocalFileInTestDir(ctx, storageClient, testDirPath, FileName1, t.T())
// 	// Generate 5 MB random data.
// 	data, err := operations.GenerateRandomData(5 * operations.MiB)
// 	if err != nil {
// 		t.T().Fatalf("Error in generating data: %v", err)
// 	}
// 	// Write first 3 MB (say A,B,C) block to file succeeds.
// 	// Fuse:[C] -> Go-SDK:[B]-> GCS[A]
// 	_, err = fh1.WriteAt(data[:3*operations.MiB], 0)
// 	assert.NoError(t.T(), err)
// 	// Write 4th 1MB (D) ensures the chunk (B) is uploaded to have enough space for C, D chunk but error may not be seen by D
// 	// Fuse:[D] -> Go-SDK:[C] -> GCS[A, B -fails upload]
// 	_, err = fh1.WriteAt(data[3*operations.MiB:4*operations.MiB], 3*operations.MiB)

// 	// Write 5th 1MB  sees error propagated via failure of B upload.
// 	_, err = fh1.WriteAt(data[4*operations.MiB:5*operations.MiB], 4*operations.MiB)
// 	assert.Error(t.T(), err)
// 	// opening new file handle succeeds.
// 	fh2 := operations.OpenFile(filePath, t.T())
// 	// writes with fh2 also fails.
// 	err = fh2.Truncate(5 * operations.MiB)
// 	assert.NoError(t.T(), err)
// 	operations.CloseFileShouldThrowError(fh2, t.T())
// 	operations.CloseFileShouldThrowError(fh1, t.T())
// }

//	func (t *uploadFailureTestSuite) TestStreamingWritesSyncWithUploadError() {
//		t.flags = []string{"--log-severity=TRACE", "--enable-streaming-writes=true", "--write-block-size-mb=1", "--write-max-blocks-per-file=1", "--custom-endpoint=" + proxyEndpoint}
//		log.Printf("Running tests with flags: %v", t.flags)
//		setup.MountGCSFuseWithGivenMountFunc(t.flags, mountFunc)
//		testDirPath = setup.SetupTestDirectory(testDirName)
//		// Create a local file.
//		_, fh1 := CreateLocalFileInTestDir(ctx, storageClient, testDirPath, FileName1, t.T())
//		// Generate 5 MB random data.
//		data, err := operations.GenerateRandomData(6 * operations.MiB)
//		if err != nil {
//			t.T().Fatalf("Error in generating data: %v", err)
//		}
//		// Emulating failure on 2nd Chunk upload.
//		// Write the first 1 MB (say A) block to the file succeeds.
//		// Fuse:[A] -> Go-SDK:[]-> GCS[]
//		_, err = fh1.WriteAt(data[:2*operations.MiB], 0)
//		assert.NoError(t.T(), err)
//		err = fh1.Sync() // Sync ensures 2 MB is uploaded.
//		assert.NoError(t.T(), err)
//		// Write the second 2 MB (say B) block to the file succeeds.
//		// Fuse:[B] -> Go-SDK:[]-> GCS[A]
//		_, err = fh1.WriteAt(data[2*operations.MiB:4*operations.MiB], 2*operations.MiB)
//		assert.NoError(t.T(), err)
//		err = fh1.Sync() // Sync ensures 2nd 1 MB (B) is uploaded and failure is propagated and returned?
//		assert.NoError(t.T(), err)
//		err = fh1.Sync() // Sync should fail now.
//		assert.NoError(t.T(), err)
//		// Write 3rd 1MB (C) block to file fails.
//		// Fuse:[] -> Go-SDK:[]-> GCS[A, B -failed]
//		_, err = fh1.WriteAt(data[4*operations.MiB:6*operations.MiB], 4*operations.MiB)
//		assert.NoError(t.T(), err)
//	}
func TestUploadFailureTestSuite(t *testing.T) {
	suite.Run(t, new(uploadFailureTestSuite))
}
