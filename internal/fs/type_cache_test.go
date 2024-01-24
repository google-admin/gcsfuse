// Copyright 2024 Google Inc. All Rights Reserved.
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

// A collection of tests for a file system where we do not attempt to write to
// the file system at all. Rather we set up contents in a GCS bucket out of
// band, wait for them to be available, and then read them via the file system.

package fs_test

import (
	"io/fs"
	"os"
	"path"
	"time"

	"github.com/googlecloudplatform/gcsfuse/internal/config"
	"github.com/googlecloudplatform/gcsfuse/internal/storage/storageutil"

	"github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
)

////////////////////////////////////////////////////////////////////////
// Common
////////////////////////////////////////////////////////////////////////

type typeCacheTestCommon struct {
	fsTest
}

var (
	// The following should be configued for different tests
	// differently inside SetUpTestSuite as these need to
	// set for mount itself.

	// ttlInSeconds is equivalent of metadata-cache:ttl-secs in config-file.
	ttlInSeconds int64

	// typeCacheMaxEntries is equivalent of metadata-cache:type-cache-max-entries in config-file.
	typeCacheMaxEntries int
)

func (t *typeCacheTestCommon) SetUpTestSuite() {
	t.serverCfg.MountConfig = config.NewMountConfig()
	t.serverCfg.MountConfig.MetadataCacheConfig = config.MetadataCacheConfig{
		TypeCacheMaxEntries: typeCacheMaxEntries,
		TtlInSeconds:        ttlInSeconds,
	}

	// logging is needed for debugging if logs need to be
	// redirected to a log file.
	logFilePath := "/tmp/type-cache-fs-composite-tests.log"
	// os.Remove(logFilePath)
	t.serverCfg.MountConfig.LogConfig = config.LogConfig{
		Severity: "TRACE",
		FilePath: logFilePath,
		Format:   "text",
		LogRotateConfig: config.LogRotateConfig{
			MaxFileSizeMB:   10240,
			BackupFileCount: 10,
			Compress:        false,
		},
	}

	// Call through.
	t.fsTest.SetUpTestSuite()
}

////////////////////////////////////////////////////////////////////////
// Specific test classes
////////////////////////////////////////////////////////////////////////

type TypeCacheTestWithMaxEntries1 struct {
	typeCacheTestCommon
}

func (t *TypeCacheTestWithMaxEntries1) SetUpTestSuite() {
	ttlInSeconds = 30
	typeCacheMaxEntries = 1

	t.typeCacheTestCommon.SetUpTestSuite()
}

type TypeCacheTestWithZeroCapacity struct {
	typeCacheTestCommon
}

func (t *TypeCacheTestWithZeroCapacity) SetUpTestSuite() {
	ttlInSeconds = 30
	typeCacheMaxEntries = 0

	t.typeCacheTestCommon.SetUpTestSuite()
}

type TypeCacheTestWithZeroTTL struct {
	typeCacheTestCommon
}

func (t *TypeCacheTestWithZeroTTL) SetUpTestSuite() {
	ttlInSeconds = 0
	typeCacheMaxEntries = 10

	t.typeCacheTestCommon.SetUpTestSuite()
}

type TypeCacheTestWithInfiniteTTL struct {
	typeCacheTestCommon
}

func (t *TypeCacheTestWithInfiniteTTL) SetUpTestSuite() {
	ttlInSeconds = -1
	typeCacheMaxEntries = 10

	t.typeCacheTestCommon.SetUpTestSuite()
}

func init() {
	RegisterTestSuite(&TypeCacheTestWithMaxEntries1{})
	RegisterTestSuite(&TypeCacheTestWithZeroCapacity{})
	RegisterTestSuite(&TypeCacheTestWithZeroTTL{})
	RegisterTestSuite(&TypeCacheTestWithInfiniteTTL{})
}

// //////////////////////////////////////////////////////////////////////
// helpers
// //////////////////////////////////////////////////////////////////////
func (t *typeCacheTestCommon) testNoInsertion() {
	const name1 = "foo"
	const contents = "taco"
	var fi fs.FileInfo
	var err error

	// Create a file object in GCS.
	fileObject, err := storageutil.CreateObject(
		ctx,
		bucket,
		name1,
		[]byte(contents))

	ExpectEq(nil, err)
	ExpectNe(nil, fileObject)

	// Stat-call with file object. It should
	// be stattable, bypassing type-cache, as a file.
	fi, err = os.Stat(path.Join(mntDir, name1))

	ExpectEq(nil, err)
	AssertNe(nil, fi)
	ExpectFalse(fi.IsDir())

	// Create a directory object in GCS with same name as the file object.
	dirObject, err := storageutil.CreateObject(
		ctx,
		bucket,
		name1+"/",
		[]byte(contents))

	ExpectEq(nil, err)
	ExpectNe(nil, dirObject)

	// Stat-call with directory object. It should
	// be stattable, bypassing type-cache, as a directory.
	// It works because no entries are inserted in type-cache
	// in this case.
	fi, err = os.Stat(path.Join(mntDir, name1) + "/")

	ExpectEq(nil, err)
	AssertNe(nil, fi)
	ExpectTrue(fi.IsDir())
}

// //////////////////////////////////////////////////////////////////////
// Tests for TypeCacheTestWithMaxEntries1
// //////////////////////////////////////////////////////////////////////
func (t *TypeCacheTestWithMaxEntries1) TestSizeBasedEviction() {
	const name1 = "foo"
	const name2 = "bar"
	const contents = "taco"
	var fi fs.FileInfo
	var err error

	// Initially, without any existing object, type-cache
	// should not contain any entry and os.Stat should fail.
	_, err = os.Stat(path.Join(mntDir, name1))

	ExpectNe(nil, err)

	// Create a file object in GCS.
	fileObject, err := storageutil.CreateObject(
		ctx,
		bucket,
		name1,
		[]byte(contents))

	ExpectEq(nil, err)
	ExpectNe(nil, fileObject)

	// Stat-call with first file object. It should
	// be stattable through type-cache as a file.
	fi, err = os.Stat(path.Join(mntDir, name1))

	ExpectEq(nil, err)
	AssertNe(nil, fi)
	ExpectFalse(fi.IsDir())

	// Create a directory object in GCS with same name as the first file object.
	dirObject, err := storageutil.CreateObject(
		ctx,
		bucket,
		name1+"/",
		[]byte(contents))

	ExpectEq(nil, err)
	ExpectNe(nil, dirObject)

	// Stat-call with the directory object. It should
	// fail there is currently an entry for the first
	// file object, which has the same name.
	_, err = os.Stat(path.Join(mntDir, name1) + "/")

	ExpectNe(nil, err)
	ExpectThat(err, oglematchers.Error(oglematchers.HasSubstr("not a directory")))

	// Create second file object in GCS.
	fileObject, err = storageutil.CreateObject(
		ctx,
		bucket,
		name2,
		[]byte(contents))

	ExpectEq(nil, err)
	ExpectNe(nil, fileObject)

	// Stat-call with the second file object will insert it into type-cache.
	// As a side-effect, this would also evict the first file object from type-cache
	// because of type-cache capacity=1 .
	fi, err = os.Stat(path.Join(mntDir, name2))

	ExpectEq(nil, err)
	AssertNe(nil, fi)
	ExpectFalse(fi.IsDir())

	// Stat-call with directory object again, to verify that the first file's
	// type-cache entry got removed, and this time type-cache inserts a directory entry
	// and stat returns a directory successfully.
	fi, err = os.Stat(path.Join(mntDir, name1) + "/")

	ExpectEq(nil, err)
	AssertNe(nil, fi)
	ExpectTrue(fi.IsDir())
}

func (t *TypeCacheTestWithMaxEntries1) TestTTLBasedEviction() {
	const name1 = "foo"
	const contents = "taco"
	var fi fs.FileInfo
	var err error

	// Create a file object in GCS.
	fileObject, err := storageutil.CreateObject(
		ctx,
		bucket,
		name1,
		[]byte(contents))

	ExpectEq(nil, err)
	ExpectNe(nil, fileObject)

	// Stat-call with existing object, found in type-cache.
	fi, err = os.Stat(path.Join(mntDir, name1))

	ExpectEq(nil, err)
	AssertNe(nil, fi)
	ExpectFalse(fi.IsDir())

	// Create a directory object in GCS with same name as the file object.
	dirObject, err := storageutil.CreateObject(
		ctx,
		bucket,
		name1+"/",
		[]byte(contents))

	ExpectEq(nil, err)
	ExpectNe(nil, dirObject)

	// Stat-call with the directory object. It should
	// fail as there is already an entry for the
	// file object, which has the same name.
	_, err = os.Stat(path.Join(mntDir, name1) + "/")

	ExpectNe(nil, err)
	ExpectThat(err, oglematchers.Error(oglematchers.HasSubstr("not a directory")))

	// Doubly confirming that the type-cache still has
	// the entry for the file object.
	fi, err = os.Stat(path.Join(mntDir, name1))

	ExpectEq(nil, err)
	AssertNe(nil, fi)
	ExpectFalse(fi.IsDir())

	// Advance time to cross TTL to let the file-object entry be
	// removed from type-cache.
	cacheClock.AdvanceTime(time.Duration(ttlInSeconds)*time.Second + time.Nanosecond)

	// Stat-call with directory object to verify that the file object's
	// type-cache entry got removed.
	fi, err = os.Stat(path.Join(mntDir, name1) + "/")

	ExpectEq(nil, err)
	AssertNe(nil, fi)
	ExpectTrue(fi.IsDir())
}

// //////////////////////////////////////////////////////////////////////
// Tests for TypeCacheTestWithZeroCapacity
// //////////////////////////////////////////////////////////////////////
func (t *TypeCacheTestWithZeroCapacity) TestNoInsertion() {
	t.typeCacheTestCommon.testNoInsertion()
}

// //////////////////////////////////////////////////////////////////////
// Tests for TypeCacheTestWithZeroTTL
// //////////////////////////////////////////////////////////////////////
func (t *TypeCacheTestWithZeroTTL) TestNoInsertion() {
	t.typeCacheTestCommon.testNoInsertion()
}

// //////////////////////////////////////////////////////////////////////
// Tests for TypeCacheTestWithInfiniteTTL
// //////////////////////////////////////////////////////////////////////
func (t *TypeCacheTestWithInfiniteTTL) TestNoTTLExpiryEver() {
	const name1 = "foo"
	const contents = "taco"
	var fi fs.FileInfo
	var err error

	// Create a file object in GCS.
	fileObject, err := storageutil.CreateObject(
		ctx,
		bucket,
		name1,
		[]byte(contents))

	ExpectEq(nil, err)
	ExpectNe(nil, fileObject)

	// Stat-call with file object. It should
	// be stattable, as a file.
	fi, err = os.Stat(path.Join(mntDir, name1))

	ExpectEq(nil, err)
	AssertNe(nil, fi)
	ExpectFalse(fi.IsDir())

	// Let 100 years pass in the type-cache's simulated clock.
	// Surely, type-cache won't forget about the file entry.
	cacheClock.AdvanceTime(100 * 365.2425 * 24 * time.Hour)

	// Create a directory object in GCS with same name as the file object.
	dirObject, err := storageutil.CreateObject(
		ctx,
		bucket,
		name1+"/",
		[]byte(contents))

	ExpectEq(nil, err)
	ExpectNe(nil, dirObject)

	// Stat-call with the directory object. It should
	// fail as there is already a type-cache entry for the
	// file object, which has the same name.
	_, err = os.Stat(path.Join(mntDir, name1) + "/")

	ExpectNe(nil, err)
	ExpectThat(err, oglematchers.Error(oglematchers.HasSubstr("not a directory")))

	// Doubly confirming that the type-cache still has
	// the entry for the file object.
	fi, err = os.Stat(path.Join(mntDir, name1))

	ExpectEq(nil, err)
	AssertNe(nil, fi)
	ExpectFalse(fi.IsDir())
}
