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

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

func parseToInt64(token string) (int64, error) {
	res, err := strconv.ParseInt(token, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("could not parse %s to int64: %v", token, err)
	}
	return res, nil
}

func readFileLineByLine(reader io.Reader) ([]string, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(content), "\n"), nil
}

func parseReadFileLog(startTimeStampSec, startTimeStampNanos int64, logs []string,
	structuredLogs map[int64]*StructuredLogEntry) error {

	// Fetch file handle, process id and inode id from the logs.
	handle, err := parseToInt64(logs[11][:len(logs[11])-1]) //Remove trailing ","
	if err != nil {
		return fmt.Errorf("file handle: %v", err)
	}
	pid, err := parseToInt64(logs[9][:len(logs[9])-1]) //Remove trailing ","
	if err != nil {
		return fmt.Errorf("process id: %v", err)
	}
	inodeID, err := parseToInt64(logs[7][:len(logs[7])-1]) //Remove trailing ","
	if err != nil {
		return fmt.Errorf("inode id: %v", err)
	}

	// ReadFile log entries can come multiple times.
	// Check if log entry exists in the map for file handle.
	// If log entry doesn't exist, add it to the map.
	_, ok := structuredLogs[handle]
	if !ok {
		structuredLogs[handle] = &StructuredLogEntry{
			Handle:           handle,
			StartTimeSeconds: startTimeStampSec,
			StartTimeNanos:   startTimeStampNanos,
			ProcessID:        pid,
			InodeID:          inodeID,
			Chunks:           []ChunkData{},
		}
	}

	return nil
}

func parseFileCacheLog(startTimeStampSec, startTimeStampNanos int64, logs []string,
	structuredLogs map[int64]*StructuredLogEntry,
	opReverseMap map[string]*handleAndChunkIndex) error {

	// Fetch operation id, file handle, size and offset from the logs.
	opID := logs[0]
	handle, err := parseToInt64(logs[8][:len(logs[8])-1]) //Remove trailing ","
	if err != nil {
		return fmt.Errorf("file handle: %v", err)
	}
	size, err := parseToInt64(logs[6])
	if err != nil {
		return fmt.Errorf("size: %v", err)
	}
	startOffset, err := parseToInt64(logs[4][:len(logs[4])-1]) //Remove trailing ","
	if err != nil {
		return fmt.Errorf("start offset: %v", err)
	}

	// Fetch the log entry for the particular file handle from the structuredLogs map.
	logEntry, ok := structuredLogs[handle]
	if !ok {
		return fmt.Errorf("ReadFile LogEntry for handle %d not found", handle)
	}

	// For the first file cache log, log entry will not have object and bucket
	// name, so populate it.
	if logEntry.ObjectName == "" && logEntry.BucketName == "" {
		bucketAndObjectName := logs[2][10 : len(logs[2])-1] // Remove prefix "FileCache(" and suffix ","
		// bucketAndObjectName will be stored in format <bucketName>:/<objectName>
		logEntry.BucketName = strings.Split(bucketAndObjectName, ":")[0]
		logEntry.ObjectName = strings.Split(bucketAndObjectName, ":")[1][1:] // Remove prefix "/"
	}

	// Create chunk data entry and append it to the filecache logs.
	chunkData := ChunkData{
		StartTimeSeconds: startTimeStampSec,
		StartTimeNanos:   startTimeStampNanos,
		StartOffset:      startOffset,
		Size:             size,
		OpID:             opID,
	}
	logEntry.Chunks = append(logEntry.Chunks, chunkData)

	// Store the file handle and chunk index in the operation reverse map.
	// This is required to map file cache response log back to log entry chunk.
	opReverseMap[opID] = &handleAndChunkIndex{handle: handle, chunkIndex: len(logEntry.Chunks) - 1}

	return nil
}

func parseFileCacheResponseLog(logs []string,
	structuredLogs map[int64]*StructuredLogEntry,
	opReverseMap map[string]*handleAndChunkIndex) error {

	opID := logs[0]
	handleAndChunkIndex, ok := opReverseMap[opID]
	if !ok {
		return fmt.Errorf("FileCache log entry not found for opID %s", opID)
	}
	handle := handleAndChunkIndex.handle
	chunkIndex := handleAndChunkIndex.chunkIndex

	// Fetch the log entry for the particular file handle from the structuredLogs map.
	logEntry, ok := structuredLogs[handle]
	if !ok {
		return fmt.Errorf("ReadFile LogEntry for handle %d not found", handle)
	}

	// Populate chunk IsSequential, CacheHit and Execution time
	chunk := &logEntry.Chunks[chunkIndex]
	chunk.IsSequential, _ = strconv.ParseBool(logs[4][:len(logs[4])-1]) //Remove trailing ","
	chunk.CacheHit, _ = strconv.ParseBool(logs[6][:len(logs[6])-1])     //Remove trailing ","
	chunk.ExecutionTime = logs[7][1 : len(logs[7])-1]                   //Remove prefix "(" and suffix ")"
	return nil
}
