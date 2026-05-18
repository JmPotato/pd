// Copyright 2026 TiKV Project Authors.
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

package main

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCaseConfigFromQueryRejectsInvalidQPS(t *testing.T) {
	_, err := caseConfigFromQuery(url.Values{"qps": []string{"bad"}})

	require.Error(t, err)
}

func TestCaseConfigFromQueryParsesQPSAndBurst(t *testing.T) {
	cfg, err := caseConfigFromQuery(url.Values{
		"qps":   []string{"12"},
		"burst": []string{"3"},
	})

	require.NoError(t, err)
	require.Equal(t, int64(12), cfg.QPS)
	require.Equal(t, int64(3), cfg.Burst)
}
