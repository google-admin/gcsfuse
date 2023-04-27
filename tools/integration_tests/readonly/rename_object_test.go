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

// Provides integration tests for file operations with --o=ro flag set.
// copy, rename, open file operations
package readonly_test

import (
	"os"
	"path"
	"testing"

	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/setup"
)

// Rename srcFile to Rename.txt
func checkIfFileRenameFailed(oldObjPath string, newObjPath string, t *testing.T) {
	_, err := os.Stat(oldObjPath)
	if err != nil {
		t.Errorf("Error in the stating object: %v", err)
	}

	if _, err := os.Stat(newObjPath); err == nil {
		t.Errorf("Renamed file %s already present", newObjPath)
	}

	err = os.Rename(oldObjPath, newObjPath)

	if err == nil {
		t.Errorf("File renamed in read-only file system.")
	}

	checkErrorForReadOnlyFileSystem(err, t)

	if _, err := os.Stat(oldObjPath); err != nil {
		t.Errorf("SrcFile is deleted in read-only file system.")
	}
	if _, err := os.Stat(newObjPath); err == nil {
		t.Errorf("Renamed file found in read-only file system.")
	}
}

// Rename testBucket/Test1.txt to testBucket/Rename.txt
func TestRenameFile(t *testing.T) {
	oldFilePath := path.Join(setup.MntDir(), FileNameInTestBucket)
	newFilePath := path.Join(setup.MntDir(), "Rename.txt")

	checkIfFileRenameFailed(oldFilePath, newFilePath, t)
}

// Rename testBucket/Test/a.txt to testBucket/Test/Rename.txt
func TestRenameFileFromSubDirectory(t *testing.T) {
	oldFilePath := path.Join(setup.MntDir(), DirectoryNameInTestBucket, FileNameInDirectoryTestBucket)
	newFilePath := path.Join(setup.MntDir(), DirectoryNameInTestBucket, "Rename.txt")

	checkIfFileRenameFailed(oldFilePath, newFilePath, t)
}

// Rename testBucket/Test to testBucket/Rename
func TestRenameDir(t *testing.T) {
	oldDirPath := path.Join(setup.MntDir(), DirectoryNameInTestBucket)
	newDirPath := path.Join(setup.MntDir(), "Rename")

	checkIfFileRenameFailed(oldDirPath, newDirPath, t)
}

// Rename testBucket/Test/b to testBucket/Test/Rename
func TestRenameSubDirectory(t *testing.T) {
	oldDirPath := path.Join(setup.MntDir(), DirectoryNameInTestBucket, SubDirectoryNameInTestBucket)
	newDirPath := path.Join(setup.MntDir(), DirectoryNameInTestBucket, "Rename")

	checkIfFileRenameFailed(oldDirPath, newDirPath, t)
}
