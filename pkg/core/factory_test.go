// Copyright 2022 TiKV Project Authors.
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

package core

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/utils/typeutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func BenchmarkDeepClone(b *testing.B) {
	for range b.N {
		src := &metapb.Region{Id: 1}
		dst := typeutil.DeepClone(src, RegionFactory)
		dst.Id = 1
	}
}

func BenchmarkProtoClone(b *testing.B) {
	clone := func(src *metapb.Region) *metapb.Region {
		dst := proto.Clone(src).(*metapb.Region)
		return dst
	}
	for range b.N {
		src := &metapb.Region{Id: 1}
		dst := clone(src)
		dst.Id = 1
	}
}
