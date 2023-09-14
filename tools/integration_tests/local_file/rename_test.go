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

// Provides integration tests for rename operation on local files.
package local_file_test

import (
	"os"
	"path"
	"testing"

	. "github.com/googlecloudplatform/gcsfuse/tools/integration_tests/local_file/helpers"
	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/setup"
)

func TestRenameOfLocalFileFails(t *testing.T) {
	// Clean the mountedDirectory before running test.
	setup.CleanMntDir()
	// Create local file with some content.
	_, fh := CreateLocalFile(FileName1, t)
	WritingToLocalFileShouldNotWriteToGCS(fh, FileName1, t)

	// Attempt to rename local file.
	err := os.Rename(path.Join(setup.MntDir(), FileName1), path.Join(setup.MntDir(), NewFileName))

	// Verify rename operation fails.
	VerifyRenameOperationNotSupported(err, t)
	// write more content to local file.
	WritingToLocalFileShouldNotWriteToGCS(fh, FileName1, t)
	// Close the local file.
	CloseFileAndValidateObjectContents(fh, FileName1, FileContents+FileContents, t)
}

func TestRenameOfDirectoryWithLocalFileFails(t *testing.T) {
	// Clean the mountedDirectory before running test.
	setup.CleanMntDir()
	//Create directory with 1 synced and 1 local file.
	CreateExplicitDirShouldNotThrowError(t)
	// Create synced file.
	NewFileShouldGetSyncedToGCSAtClose(path.Join(ExplicitDirName, FileName1), t)
	// Create local file with some content.
	_, fh := CreateLocalFile(path.Join(ExplicitDirName, FileName2), t)
	WritingToLocalFileShouldNotWriteToGCS(fh, path.Join(ExplicitDirName, FileName2), t)

	// Attempt to rename directory containing local file.
	err := os.Rename(path.Join(setup.MntDir(), ExplicitDirName), path.Join(setup.MntDir(), NewDirName))

	// Verify rename operation fails.
	VerifyRenameOperationNotSupported(err, t)
	// write more content to local file.
	WritingToLocalFileShouldNotWriteToGCS(fh, FileName2, t)
	// Close the local file.
	CloseFileAndValidateObjectContents(fh, path.Join(ExplicitDirName, FileName2),
		FileContents+FileContents, t)
}

func TestRenameOfLocalFileSucceedsAfterSync(t *testing.T) {
	TestRenameOfLocalFileFails(t)

	// Attempt to Rename synced file.
	err := os.Rename(path.Join(setup.MntDir(), FileName1), path.Join(setup.MntDir(), NewFileName))

	// Validate.
	if err != nil {
		t.Fatalf("os.Rename() failed on synced file: %v", err)
	}
	ValidateObjectContents(NewFileName, FileContents+FileContents, t)
	ValidateObjectNotFoundErrOnGCS(FileName1, t)
}

func TestRenameOfDirectoryWithLocalFileSucceedsAfterSync(t *testing.T) {
	TestRenameOfDirectoryWithLocalFileFails(t)

	// Attempt to rename directory again after sync.
	err := os.Rename(path.Join(setup.MntDir(), ExplicitDirName), path.Join(setup.MntDir(), NewDirName))

	// Validate.
	if err != nil {
		t.Fatalf("os.Rename() failed on directory containing synced files: %v", err)
	}
	ValidateObjectContents(path.Join(NewDirName, FileName1), FileContents, t)
	ValidateObjectNotFoundErrOnGCS(path.Join(ExplicitDirName, FileName1), t)
	ValidateObjectContents(path.Join(NewDirName, FileName2), FileContents+FileContents, t)
	ValidateObjectNotFoundErrOnGCS(path.Join(ExplicitDirName, FileName2), t)
}
