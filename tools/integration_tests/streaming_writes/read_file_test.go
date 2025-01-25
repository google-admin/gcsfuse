package streaming_writes

import (
	"path"

	. "github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/client"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/operations"
	"github.com/stretchr/testify/require"
)

func (t *defaultMountCommonTest) TestReadBeforeFileIsFlushed() {
	testContent := "testContent"
	// Write data to file.
	operations.WriteAt(testContent, 0, t.f1, t.T())

	// Try to read the file.
	_, err := t.f1.Seek(0, 0)
	require.NoError(t.T(), err)
	buf := make([]byte, 10)
	_, err = t.f1.Read(buf)

	require.Error(t.T(), err, "input/output error")
	// Validate if correct content is uploaded to GCS after read error.
	CloseFileAndValidateContentFromGCS(ctx, storageClient, t.f1, testDirName, t.fileName, testContent, t.T())
}

func (t *defaultMountCommonTest) TestReadAfterFlush() {
	testContent := "testContent"
	// Write data to file and flush.
	operations.WriteAt(testContent, 0, t.f1, t.T())
	CloseFileAndValidateContentFromGCS(ctx, storageClient, t.f1, testDirName, t.fileName, testContent, t.T())

	// Perform read and validate the contents.
	var err error
	t.f1, err = operations.OpenFileAsReadonly(path.Join(testDirPath, t.fileName))
	require.NoError(t.T(), err)
	buf := make([]byte, len(testContent))
	_, err = t.f1.Read(buf)

	require.NoError(t.T(), err)
	require.Equal(t.T(), string(buf), testContent)
}
