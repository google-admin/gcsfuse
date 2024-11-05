// Copyright 2024 Google LLC
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

package common

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoinShutdownFunc(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		fns          []ShutdownFn
		expectedErrs []string
	}{
		{
			name:         "normal",
			fns:          []ShutdownFn{func(_ context.Context) error { return nil }},
			expectedErrs: nil,
		},
		{
			name:         "one_err",
			fns:          []ShutdownFn{func(_ context.Context) error { return fmt.Errorf("err") }},
			expectedErrs: []string{"err"},
		},
		{
			name: "two_err",
			fns: []ShutdownFn{
				func(_ context.Context) error { return fmt.Errorf("err1") },
				func(_ context.Context) error { return fmt.Errorf("err2") },
			},
			expectedErrs: []string{"err1", "err2"},
		},
		{
			name: "two_err_one_normal",
			fns: []ShutdownFn{
				func(_ context.Context) error { return fmt.Errorf("err1") },
				func(_ context.Context) error { return nil },
				func(_ context.Context) error { return fmt.Errorf("err2") },
			},
			expectedErrs: []string{"err1", "err2"},
		},
		{
			name: "nil",
			fns: []ShutdownFn{
				func(_ context.Context) error { return fmt.Errorf("err1") },
				nil,
				func(_ context.Context) error { return fmt.Errorf("err2") },
			},
			expectedErrs: []string{"err1", "err2"},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := JoinShutdownFunc(tc.fns...)(context.Background())

			if len(tc.expectedErrs) == 0 {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				for _, e := range tc.expectedErrs {
					assert.ErrorContains(t, err, e)
				}
			}
		})
	}
}
