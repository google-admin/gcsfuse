package downloader

import (
	"io"
	"math"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/googlecloudplatform/gcsfuse/v2/internal/storage/fake"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"

	"github.com/googlecloudplatform/gcsfuse/v2/cfg"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/cache/data"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/cache/lru"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/cache/util"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/storage"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/storage/gcs"
	testutil "github.com/googlecloudplatform/gcsfuse/v2/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type JobTestifyTest struct {
	suite.Suite
	ctx                    context.Context
	defaultFileCacheConfig *cfg.FileCacheConfig
	job                    *Job
	object                 gcs.MinObject
	cache                  *lru.Cache
	fileSpec               data.FileSpec
	mockBucket             *storage.TestifyMockBucket
}

func TestJobTestifyTestSuite(testSuite *testing.T) { suite.Run(testSuite, new(JobTestifyTest)) }

func (t *JobTestifyTest) initReadCacheTestifyTest(objectName string, objectContent []byte, sequentialReadSize int32, lruCacheSize uint64, removeCallback func()) {
	// mock stat object call
	minObject := gcs.MinObject{
		Name: objectName,
		Size: uint64(len(objectContent)),
	}
	t.object = minObject
	t.fileSpec = data.FileSpec{
		Path:     path.Join(path.Join(os.Getenv("HOME"), "cache/dir"), t.object.Name),
		FilePerm: util.DefaultFilePerm,
		DirPerm:  util.DefaultDirPerm,
	}
	t.cache = lru.NewCache(lruCacheSize)
	t.job = NewJob(&t.object, t.mockBucket, t.cache, sequentialReadSize, t.fileSpec, removeCallback, t.defaultFileCacheConfig, semaphore.NewWeighted(math.MaxInt64), nil)
	fileInfoKey := data.FileInfoKey{
		BucketName: storage.TestBucketName,
		ObjectName: objectName,
	}
	fileInfo := data.FileInfo{
		Key:              fileInfoKey,
		ObjectGeneration: t.object.Generation,
		FileSize:         t.object.Size,
		Offset:           0,
	}
	fileInfoKeyName, err := fileInfoKey.Key()
	assert.Equal(t.T(), nil, err)
	_, err = t.cache.Insert(fileInfoKeyName, fileInfo)
	assert.Equal(t.T(), nil, err)
}

func (t *JobTestifyTest) SetupTest() {
	t.ctx, _ = context.WithCancel(context.Background())
	t.mockBucket = new(storage.TestifyMockBucket)
}

func (t *JobTestifyTest) Test_downloadObjectToFile_WithReadHandle() {
	objectName := "path/in/gcs/foo.txt"
	objectSize := 10 * util.MiB
	objectContent := testutil.GenerateRandomBytes(objectSize)
	t.initReadCacheTestifyTest(objectName, objectContent, 5, uint64(2*objectSize), func() {})
	t.job.cancelCtx, t.job.cancelFunc = context.WithCancel(context.Background())
	file, err := util.CreateFile(data.FileSpec{Path: t.job.fileSpec.Path,
		FilePerm: os.FileMode(0600), DirPerm: os.FileMode(0700)}, os.O_TRUNC|os.O_RDWR)
	defer func() {
		_ = file.Close()
	}()
	// Add subscriber
	subscribedOffset := int64(10 * util.MiB)
	notificationC := t.job.subscribe(subscribedOffset)
	assert.Equal(t.T(), nil, err)
	rc := io.NopCloser(strings.NewReader(string(objectContent)))
	rd := &fake.FakeReader{ReadCloser: rc, Handle: []byte("opaque-handle")}
	t.mockBucket.On("Name").Return(storage.TestBucketName)
	readObjectReq := gcs.ReadObjectRequest{Name: objectName, Generation: 0, Range: &gcs.ByteRange{Start: 0, Limit: 5 * util.MiB}, ReadCompressed: false, ReadHandle: nil}
	t.mockBucket.On("NewReaderWithReadHandle", mock.Anything, &readObjectReq).Return(rd, nil)
	readObjectReq2 := gcs.ReadObjectRequest{Name: objectName, Generation: 0, Range: &gcs.ByteRange{Start: 5 * util.MiB, Limit: 10 * util.MiB}, ReadCompressed: false, ReadHandle: []byte("opaque-handle")}
	t.mockBucket.On("NewReaderWithReadHandle", mock.Anything, &readObjectReq2).Return(rd, nil)

	// Start download
	err = t.job.downloadObjectToFile(file)

	t.mockBucket.AssertExpectations(t.T())
	assert.Nil(t.T(), err)
	jobStatus, ok := <-notificationC
	assert.Equal(t.T(), true, ok)
	// Check the notification is sent after subscribed offset
	assert.GreaterOrEqual(t.T(), jobStatus.Offset, subscribedOffset)
	t.job.mu.Lock()
	defer t.job.mu.Unlock()
	// Verify file is downloaded
	verifyCompleteFile(t.T(), t.fileSpec, objectContent)
	// Verify fileInfoCache update
	verifyFileInfoEntry(t.T(), t.mockBucket, t.object, t.cache, uint64(objectSize))
}
