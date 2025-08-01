// Copyright 2017 TiKV Project Authors.
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

package api

import (
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/tests"
)

type statusTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestStatusTestSuite(t *testing.T) {
	suite.Run(t, new(statusTestSuite))
}

func (suite *statusTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *statusTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *statusTestSuite) TestStatus() {
	suite.env.RunTest(suite.checkStatus)
}

func (suite *statusTestSuite) checkStatus(cluster *tests.TestCluster) {
	re := suite.Require()
	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1"

	addr := urlPrefix + "/status"
	resp, err := tests.TestDialClient.Get(addr)
	re.NoError(err)
	buf, err := io.ReadAll(resp.Body)
	re.NoError(err)
	checkStatusResponse(re, buf)
	resp.Body.Close()
}

func checkStatusResponse(re *require.Assertions, body []byte) {
	got := versioninfo.Status{}
	re.NoError(json.Unmarshal(body, &got))
	re.Equal(versioninfo.PDBuildTS, got.BuildTS)
	re.Equal(versioninfo.PDGitHash, got.GitHash)
	re.Equal(versioninfo.PDReleaseVersion, got.Version)
	re.Equal(versioninfo.PDKernelType, got.KernelType)
}
