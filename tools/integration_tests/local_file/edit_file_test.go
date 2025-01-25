package local_file_test

import (
	"os"
	"path"

	. "github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/client"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/operations"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/setup"
	"github.com/stretchr/testify/require"
)

func (t *CommonLocalFileTestSuite) TestEditsToNewlyCreatedFile() {
	testDirPath = setup.SetupTestDirectory(testDirName)
	// Create a local file.
	_, fh := CreateLocalFileInTestDir(ctx, storageClient, testDirPath, FileName1, t.T())
	// Write some contents to file sequentially.
	for i := 0; i < 3; i++ {
		operations.WriteWithoutClose(fh, FileContents, t.T())
	}
	// Close the file and validate that the file is created on GCS.
	expectedContent := FileContents + FileContents + FileContents
	CloseFileAndValidateContentFromGCS(ctx, storageClient, fh, testDirName, FileName1, expectedContent, t.T())

	// Perform edit
	fhNew := operations.OpenFile(path.Join(testDirPath, FileName1), t.T())
	newContent := "newContent"
	_, err := fhNew.WriteAt([]byte(newContent), 0)

	require.Nil(t.T(), err)
	CloseFileAndValidateContentFromGCS(ctx, storageClient, fhNew, testDirName, FileName1, newContent+FileContents+FileContents, t.T())
}

func (t *CommonLocalFileTestSuite) TestAppendsToNewlyCreatedFile() {
	testDirPath = setup.SetupTestDirectory(testDirName)
	// Create a local file.
	_, fh := CreateLocalFileInTestDir(ctx, storageClient, testDirPath, FileName1, t.T())
	// Write some contents to file sequentially.
	for i := 0; i < 3; i++ {
		operations.WriteWithoutClose(fh, FileContents, t.T())
	}
	// Close the file and validate that the file is created on GCS.
	expectedContent := FileContents + FileContents + FileContents
	CloseFileAndValidateContentFromGCS(ctx, storageClient, fh, testDirName, FileName1, expectedContent, t.T())

	// Append to the file.
	fhNew, err := os.OpenFile(path.Join(testDirPath, FileName1), os.O_RDWR|os.O_APPEND, operations.FilePermission_0777)
	require.Nil(t.T(), err)
	appendedContent := "appendedContent"
	_, err = fhNew.Write([]byte(appendedContent))

	require.Nil(t.T(), err)
	CloseFileAndValidateContentFromGCS(ctx, storageClient, fhNew, testDirName, FileName1, expectedContent+appendedContent, t.T())
}
