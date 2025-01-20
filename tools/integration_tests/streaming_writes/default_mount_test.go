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
	"os"

	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/setup"
	"github.com/stretchr/testify/suite"
)

type defaultMountCommonTest struct {
	f1       *os.File
	fileName string
	suite.Suite
}

func (t *defaultMountCommonTest) SetupSuite() {
	flags := []string{"--enable-streaming-writes=true", "--write-block-size-mb=1", "--write-max-blocks-per-file=2"}
	setup.MountGCSFuseWithGivenMountFunc(flags, mountFunc)
	testDirPath = setup.SetupTestDirectory(testDirName)
}

func (t *defaultMountCommonTest) TearDownSuite() {
	setup.UnmountGCSFuse(rootDir)
}