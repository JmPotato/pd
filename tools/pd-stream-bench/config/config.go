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

package config

import (
	"strings"

	"github.com/pingcap/errors"
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/utils/configutil"
)

// Config is the pd-stream-bench configuration.
type Config struct {
	flagSet    *flag.FlagSet
	configFile string

	// PDAddr is the toml `pd` field. It accepts either a single endpoint or a
	// comma-separated list (e.g. "https://A:2379,https://B:2379,https://C:2379"). After
	// Adjust() runs, PDAddr is canonicalized to the FIRST endpoint, and Endpoints holds
	// the full normalized list. Use EndpointFor(workerID) to round-robin across them.
	PDAddr     string `toml:"pd" json:"pd"`
	StatusAddr string `toml:"status-addr" json:"status-addr"`
	CAPath     string `toml:"cacert" json:"cacert"`
	CertPath   string `toml:"cert" json:"cert"`
	KeyPath    string `toml:"key" json:"key"`
	CheckOnly  bool   `toml:"check-only" json:"check-only"`

	// Endpoints is the parsed multi-endpoint list. Not toml-exposed (derived from PDAddr
	// in Adjust). Always non-empty after a successful Parse(). v2.1 (2026-05-20): added to
	// let workers distribute long-lived gRPC streams across all 3 PD instances; without
	// this every stream lands on PD0 and the leader's goroutine count blows past the 5.8k
	// target (5/18 run saw 7,317 at the precheck snapshot).
	Endpoints []string `toml:"-" json:"endpoints"`

	MetaStorageWatchStreams    int `toml:"metastorage-watch-streams" json:"metastorage-watch-streams"`
	AcquireTokenBucketsStreams int `toml:"acquire-token-buckets-streams" json:"acquire-token-buckets-streams"`
	EtcdWatchStreams           int `toml:"etcd-watch-streams" json:"etcd-watch-streams"`
	LeaseKeepaliveStreams      int `toml:"lease-keepalive-streams" json:"lease-keepalive-streams"`
	ConnectionFanoutTarget     int `toml:"connection-fanout-target" json:"connection-fanout-target"`
	StreamRequestIntervalMS    int `toml:"stream-request-interval-ms" json:"stream-request-interval-ms"`
}

// EndpointFor returns the PD endpoint assigned to workerID via round-robin across the
// configured Endpoints list. Always returns a non-empty endpoint after Adjust() runs.
func (c *Config) EndpointFor(workerID int) string {
	if len(c.Endpoints) == 0 {
		return c.PDAddr
	}
	if workerID < 0 {
		workerID = -workerID
	}
	return c.Endpoints[workerID%len(c.Endpoints)]
}

// NewConfig returns a config with flags registered.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("pd-stream-bench", flag.ContinueOnError)
	fs := cfg.flagSet
	fs.StringVar(&cfg.configFile, "config", "", "config file")
	fs.StringVar(&cfg.PDAddr, "pd", "http://127.0.0.1:2379", "pd address")
	fs.StringVar(&cfg.StatusAddr, "status-addr", "127.0.0.1:20181", "status server address")
	fs.StringVar(&cfg.CAPath, "cacert", "", "path of file that contains list of trusted TLS CAs")
	fs.StringVar(&cfg.CertPath, "cert", "", "path of file that contains X509 certificate in PEM format")
	fs.StringVar(&cfg.KeyPath, "key", "", "path of file that contains X509 key in PEM format")
	fs.BoolVar(&cfg.CheckOnly, "check-only", false, "check configured stream capabilities and exit")
	fs.IntVar(&cfg.MetaStorageWatchStreams, "metastorage-watch-streams", 0, "MetaStorage.Watch stream count")
	fs.IntVar(&cfg.AcquireTokenBucketsStreams, "acquire-token-buckets-streams", 0, "ResourceManager.AcquireTokenBuckets stream count")
	fs.IntVar(&cfg.EtcdWatchStreams, "etcd-watch-streams", 0, "etcd watch stream count")
	fs.IntVar(&cfg.LeaseKeepaliveStreams, "lease-keepalive-streams", 0, "lease keepalive stream count")
	fs.IntVar(&cfg.ConnectionFanoutTarget, "connection-fanout-target", 0, "target total gRPC connection count")
	fs.IntVar(&cfg.StreamRequestIntervalMS, "stream-request-interval-ms", 1000, "stream request interval in milliseconds")
	return cfg
}

// Parse parses flags and config file.
func (c *Config) Parse(arguments []string) error {
	if err := c.flagSet.Parse(arguments); err != nil {
		return errors.WithStack(err)
	}
	if c.configFile != "" {
		if _, err := configutil.ConfigFromFile(c, c.configFile); err != nil {
			return err
		}
	}
	if err := c.flagSet.Parse(arguments); err != nil {
		return errors.WithStack(err)
	}
	if len(c.flagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.flagSet.Arg(0))
	}
	c.Adjust()
	return c.Validate()
}

// Adjust fills defaults and splits PDAddr's CSV form into Endpoints.
func (c *Config) Adjust() {
	c.Endpoints = c.Endpoints[:0]
	for _, part := range strings.Split(c.PDAddr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		c.Endpoints = append(c.Endpoints, normalizePDAddr(part))
	}
	if len(c.Endpoints) > 0 {
		c.PDAddr = c.Endpoints[0]
	} else {
		c.PDAddr = normalizePDAddr(c.PDAddr)
	}
	if c.StreamRequestIntervalMS == 0 {
		c.StreamRequestIntervalMS = 1000
	}
}

// Validate validates stream counts.
func (c *Config) Validate() error {
	if len(c.Endpoints) == 0 {
		return errors.Errorf("pd endpoint list is empty (set --pd or toml `pd = \"https://host:port[,...]\"`)")
	}
	if c.MetaStorageWatchStreams < 0 {
		return errors.Errorf("metastorage-watch-streams can not be negative")
	}
	if c.AcquireTokenBucketsStreams < 0 {
		return errors.Errorf("acquire-token-buckets-streams can not be negative")
	}
	if c.EtcdWatchStreams < 0 {
		return errors.Errorf("etcd-watch-streams can not be negative")
	}
	if c.LeaseKeepaliveStreams < 0 {
		return errors.Errorf("lease-keepalive-streams can not be negative")
	}
	if c.ConnectionFanoutTarget < 0 {
		return errors.Errorf("connection-fanout-target can not be negative")
	}
	if c.StreamRequestIntervalMS < 0 {
		return errors.Errorf("stream-request-interval-ms can not be negative")
	}
	return nil
}

func normalizePDAddr(addr string) string {
	if addr == "" || strings.Contains(addr, "://") {
		return addr
	}
	return "http://" + addr
}
