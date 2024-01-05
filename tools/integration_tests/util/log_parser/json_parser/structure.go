// Copyright 2023 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http:#www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package json_parser

// StructuredLogEntry stores the structured format to be created from logs.
type StructuredLogEntry struct {
	Handle     int64
	StartTime  int64
	ProcessID  int64
	InodeID    int64
	BucketName string
	ObjectName string
	// It can be safely assumed that the Chunks will be sorted on timestamp as logs
	// are parsed in the order of timestamps.
	Chunks []ChunkData
}

// ChunkData stores the format of chunk to be stored StructuredLogEntry.
type ChunkData struct {
	StartTime     int64
	StartOffset   int64
	Size          int64
	CacheHit      bool
	IsSequential  bool
	OpID          string
	ExecutionTime string
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

// handleAndChunkIndex is used to store reverse mapping of FileCache operation id to
// handle and chunk index stored in structure.
type handleAndChunkIndex struct {
	handle     int64
	chunkIndex int
}
