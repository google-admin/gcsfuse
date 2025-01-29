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

package local_file

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/client"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/operations"
)

const (
	onlyDirMounted       = "OnlyDirMountLocalFiles"
	testDirLocalFileTest = "LocalFileTest"
)

var (
	storageClient *storage.Client
	ctx           context.Context
)

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func (t *CommonLocalFileTestSuite) WritingToLocalFileShouldNotWriteToGCS(ctx context.Context, storageClient *storage.Client, testDirName string) {
	operations.WriteWithoutClose(t.fh, client.FileContents, t.T())
	client.ValidateObjectNotFoundErrOnGCS(ctx, storageClient, testDirName, t.fileName, t.T())
}

func (t *CommonLocalFileTestSuite) NewFileShouldGetSyncedToGCSAtClose(ctx context.Context, storageClient *storage.Client) {
	// Writing contents to local file shouldn't create file on GCS.
	testDirName := client.GetDirName(t.testDirPath)
	t.WritingToLocalFileShouldNotWriteToGCS(ctx, storageClient, testDirName)

	// Close the file and validate if the file is created on GCS.
	client.CloseFileAndValidateContentFromGCS(ctx, storageClient, t.fh, testDirName, t.fileName, client.FileContents, t.T())
}
