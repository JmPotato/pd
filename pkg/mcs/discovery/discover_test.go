// Copyright 2023 TiKV Project Authors.
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

package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestDiscover(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()
	sr1 := NewServiceRegister(context.Background(), client, "test_service", "127.0.0.1:1", "127.0.0.1:1", DefaultLeaseInSeconds)
	err := sr1.Register()
	re.NoError(err)
	sr2 := NewServiceRegister(context.Background(), client, "test_service", "127.0.0.1:2", "127.0.0.1:2", DefaultLeaseInSeconds)
	err = sr2.Register()
	re.NoError(err)

	endpoints, err := Discover(client, "test_service")
	re.NoError(err)
	re.Len(endpoints, 2)
	re.Equal("127.0.0.1:1", endpoints[0])
	re.Equal("127.0.0.1:2", endpoints[1])

	sr1.cancel()
	sr2.cancel()
	time.Sleep(DefaultLeaseInSeconds * time.Second) // ensure that the lease is expired
	time.Sleep(500 * time.Millisecond)              // wait for the etcd to clean up the expired keys
	endpoints, err = Discover(client, "test_service")
	re.NoError(err)
	re.Empty(endpoints)
}

func TestServiceRegistryEntry(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()
	entry1 := &ServiceRegistryEntry{ServiceAddr: "127.0.0.1:1"}
	s1, err := entry1.Serialize()
	re.NoError(err)
	sr1 := NewServiceRegister(context.Background(), client, "test_service", "127.0.0.1:1", s1, DefaultLeaseInSeconds)
	err = sr1.Register()
	re.NoError(err)
	entry2 := &ServiceRegistryEntry{ServiceAddr: "127.0.0.1:2"}
	s2, err := entry2.Serialize()
	re.NoError(err)
	sr2 := NewServiceRegister(context.Background(), client, "test_service", "127.0.0.1:2", s2, DefaultLeaseInSeconds)
	err = sr2.Register()
	re.NoError(err)

	endpoints, err := Discover(client, "test_service")
	re.NoError(err)
	re.Len(endpoints, 2)
	returnedEntry1 := &ServiceRegistryEntry{}
	err = returnedEntry1.Deserialize([]byte(endpoints[0]))
	re.NoError(err)
	re.Equal("127.0.0.1:1", returnedEntry1.ServiceAddr)
	returnedEntry2 := &ServiceRegistryEntry{}
	err = returnedEntry2.Deserialize([]byte(endpoints[1]))
	re.NoError(err)
	re.Equal("127.0.0.1:2", returnedEntry2.ServiceAddr)

	sr1.cancel()
	sr2.cancel()
	time.Sleep(DefaultLeaseInSeconds * time.Second) // ensure that the lease is expired
	time.Sleep(500 * time.Millisecond)              // wait for the etcd to clean up the expired keys
	endpoints, err = Discover(client, "test_service")
	re.NoError(err)
	re.Empty(endpoints)
}
