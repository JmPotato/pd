// Copyright 2024 TiKV Project Authors.
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
	"github.com/pingcap/log"
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/tools/pd-api-bench/cases"
	"github.com/tikv/pd/tools/pd-api-bench/filler"
	"go.uber.org/zap"
)

// Config is the api-bench configuration.
type Config struct {
	flagSet    *flag.FlagSet
	configFile string
	PDAddr     string `toml:"pd" json:"pd"`
	StatusAddr string `toml:"status" json:"status"`
	KeyFormat  string `toml:"key-format" json:"key-format"`
	Mode       string `toml:"mode" json:"mode"`

	Log      log.Config `toml:"log" json:"log"`
	Logger   *zap.Logger
	LogProps *log.ZapProperties

	Client int64 `toml:"client" json:"client"`

	// tls
	CaPath   string `toml:"ca-path" json:"ca-path"`
	CertPath string `toml:"cert-path" json:"cert-path"`
	KeyPath  string `toml:"key-path" json:"key-path"`

	// only for init
	HTTP map[string]cases.Config `toml:"http" json:"http"`
	GRPC map[string]cases.Config `toml:"grpc" json:"grpc"`
	Etcd map[string]cases.Config `toml:"etcd" json:"etcd"`

	Namespace             string `toml:"namespace" json:"namespace"`
	TargetDBSizeBytes     int64  `toml:"target-db-size-bytes" json:"target-db-size-bytes"`
	KeyShards             int    `toml:"key-shards" json:"key-shards"`
	ValueSizeBytes        int    `toml:"value-size-bytes" json:"value-size-bytes"`
	WriteQPSDuringPrefill int    `toml:"write-qps-during-prefill" json:"write-qps-during-prefill"`
	SteadyPutQPS          int    `toml:"steady-put-qps" json:"steady-put-qps"`
	SteadyTxnQPS          int    `toml:"steady-txn-qps" json:"steady-txn-qps"`
}

// NewConfig return a set of settings.
func NewConfig(flagSet *flag.FlagSet) *Config {
	cfg := &Config{}
	cfg.flagSet = flagSet
	fs := cfg.flagSet
	fs.StringVar(&cfg.configFile, "config", "", "config file")
	fs.StringVar(&cfg.PDAddr, "pd", "http://127.0.0.1:2379", "pd address")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "", "log file path")
	fs.StringVar(&cfg.StatusAddr, "status", "127.0.0.1:10081", "status address")
	fs.StringVar(&cfg.KeyFormat, "key-format", "raw", "lookup key format: raw or table")
	fs.StringVar(&cfg.Mode, "mode", "bench", "run mode: bench, etcd-filler, or etcd-filler-clean")
	fs.Int64Var(&cfg.Client, "client", 1, "client number")
	fs.StringVar(&cfg.CaPath, "cacert", "", "path of file that contains list of trusted SSL CAs")
	fs.StringVar(&cfg.CertPath, "cert", "", "path of file that contains X509 certificate in PEM format")
	fs.StringVar(&cfg.KeyPath, "key", "", "path of file that contains X509 key in PEM format")
	fs.StringVar(&cfg.Namespace, "namespace", filler.DefaultNamespace, "etcd filler namespace")
	fs.Int64Var(&cfg.TargetDBSizeBytes, "target-db-size-bytes", 0, "etcd filler target byte estimate")
	fs.IntVar(&cfg.KeyShards, "key-shards", 1024, "etcd filler key shards")
	fs.IntVar(&cfg.ValueSizeBytes, "value-size-bytes", 4096, "etcd filler value size in bytes")
	fs.IntVar(&cfg.WriteQPSDuringPrefill, "write-qps-during-prefill", 0, "etcd filler prefill write qps")
	fs.IntVar(&cfg.SteadyPutQPS, "steady-put-qps", 0, "etcd filler steady Put qps")
	fs.IntVar(&cfg.SteadyTxnQPS, "steady-txn-qps", 0, "etcd filler steady Txn qps")
	return cfg
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	// Load config file if specified.
	if c.configFile != "" {
		_, err = configutil.ConfigFromFile(c, c.configFile)
		if err != nil {
			return err
		}
	}
	c.Adjust()

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	return nil
}

// InitCoordinator set case config from config itself.
func (c *Config) InitCoordinator(co *cases.Coordinator) error {
	var errs []string
	for name, cfg := range c.HTTP {
		err := co.SetHTTPCase(name, &cfg)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	for name, cfg := range c.GRPC {
		err := co.SetGRPCCase(name, &cfg)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	for name, cfg := range c.Etcd {
		err := co.SetEtcdCase(name, &cfg)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.Errorf("init coordinator failed: %s", strings.Join(errs, "; "))
	}
	return nil
}

// Adjust is used to adjust configurations
func (c *Config) Adjust() {
	if len(c.Log.Format) == 0 {
		c.Log.Format = "text"
	}
	if len(c.KeyFormat) == 0 {
		c.KeyFormat = "raw"
	}
	if len(c.Mode) == 0 {
		c.Mode = "bench"
	}
	fillerCfg := c.FillerConfig()
	fillerCfg.Adjust()
	c.Namespace = fillerCfg.Namespace
	c.KeyShards = fillerCfg.KeyShards
	c.ValueSizeBytes = fillerCfg.ValueSizeBytes
}

// FillerConfig returns the etcd filler mode config.
func (c *Config) FillerConfig() filler.Config {
	return filler.Config{
		Namespace:             c.Namespace,
		TargetDBSizeBytes:     c.TargetDBSizeBytes,
		KeyShards:             c.KeyShards,
		ValueSizeBytes:        c.ValueSizeBytes,
		WriteQPSDuringPrefill: c.WriteQPSDuringPrefill,
		SteadyPutQPS:          c.SteadyPutQPS,
		SteadyTxnQPS:          c.SteadyTxnQPS,
	}
}
