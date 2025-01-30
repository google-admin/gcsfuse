// Copyright 2023 Google LLC
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

package storageutil

import (
	"cloud.google.com/go/storage"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/logger"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var count = 0

func ShouldRetry(err error) (b bool) {
	b = storage.ShouldRetry(err)
	if b {
		if typed, ok := err.(*googleapi.Error); ok {
			if typed.Code == 503 && count == 0 {
				count++
				b = false
				logger.Infof("Not Retrying for error: %v", err)
				return
			}
		}
		if b {
			logger.Infof("Retrying for error: %v", err)
			return
		}
		return
	}

	// HTTP 401 errors - Invalid Credentials
	// This is a work-around to fix the corner case where GCSFuse checks the token
	// as valid but GCS says invalid. This might be due to client-server timer
	// issues. Actual fix will be refresh the token earlier than 1 hr.
	// Changes will be done post resolution of the below issue:
	// https://github.com/golang/oauth2/issues/623
	// TODO: Please incorporate the correct fix post resolution of the above issue.
	if typed, ok := err.(*googleapi.Error); ok {
		if typed.Code == 401 {
			b = true
			logger.Infof("Retrying for error-code 401: %v", err)
			return
		}
	}

	// This is the same case as above, but for gRPC UNAUTHENTICATED errors. See
	// https://github.com/golang/oauth2/issues/623
	// TODO: Please incorporate the correct fix post resolution of the above issue.
	if status, ok := status.FromError(err); ok {
		if status.Code() == codes.Unauthenticated {
			b = true
			logger.Infof("Retrying for UNAUTHENTICATED error: %v", err)
			return
		}
	}
	return
}
