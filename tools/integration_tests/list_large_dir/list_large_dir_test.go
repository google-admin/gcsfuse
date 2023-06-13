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

// Provide test for listing large directory
package list_large_dir_test

import (
	"log"
	"os"
	"path"
	"testing"

	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/mounting/static_mounting"
	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/setup"
)

const DirectoryWithTwelveThousandFiles = "directoryWithTwelveThousandFiles"
const PrefixFileInDirectoryWithTwelveThousandFiles = "fileInDirectoryWithTwelveThousandFiles"
const PrefixExplicitDirInLargeDirListTest = "explicitDirInLargeDirListTest"
const PrefixImplicitDirInLargeDirListTest = "implicitDirInLargeDirListTest"
const NumberOfFilesInDirectoryWithTwelveThousandFiles = 12000
const NumberOfImplicitDirsInDirectoryWithTwelveThousandFiles = 100
const NumberOfExplicitDirsInDirectoryWithTwelveThousandFiles = 100

func TestMain(m *testing.M) {
	setup.ParseSetUpFlags()

	flags := [][]string{{"--implicit-dirs"}}

	if setup.TestBucket() != "" && setup.MountedDirectory() != "" {
		log.Printf("Both --testbucket and --mountedDirectory can't be specified at the same time.")
		os.Exit(1)
	}

	// Create twelve thousand files in the directoryWithTwelveThousandFiles directory.
	dirPath := path.Join(setup.TestBucket(), DirectoryWithTwelveThousandFiles)
	setup.RunScriptForTestData("testdata/create_twelve_thousand_files.sh", dirPath)

	// Run tests for mountedDirectory only if --mountedDirectory flag is set.
	setup.RunTestsForMountedDirectoryFlag(m)

	setup.SetUpTestDirForTestBucketFlag()

	successCode := static_mounting.RunTests(flags, m)

	os.Exit(successCode)
}
