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

package cases

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	pdHttp "github.com/tikv/pd/client/http"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var base = int64(time.Second) / int64(time.Microsecond)

// Coordinator managers the operation of the gRPC and HTTP case.
type Coordinator struct {
	ctx context.Context

	httpClients []pdHttp.Client
	gRPCClients []pd.Client
	etcdClients []*clientv3.Client

	http       map[string]*httpController
	grpc       map[string]*gRPCController
	directGRPC map[string]*directGRPCController
	etcd       map[string]*etcdController

	mu sync.RWMutex
}

// NewCoordinator returns a new coordinator.
func NewCoordinator(ctx context.Context, httpClients []pdHttp.Client, gRPCClients []pd.Client, etcdClients []*clientv3.Client) *Coordinator {
	return &Coordinator{
		ctx:         ctx,
		httpClients: httpClients,
		gRPCClients: gRPCClients,
		etcdClients: etcdClients,
		http:        make(map[string]*httpController),
		grpc:        make(map[string]*gRPCController),
		directGRPC:  make(map[string]*directGRPCController),
		etcd:        make(map[string]*etcdController),
	}
}

// GetHTTPCase returns the HTTP case config.
func (c *Coordinator) GetHTTPCase(name string) (*Config, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if controller, ok := c.http[name]; ok {
		return controller.getConfig(), nil
	}
	return nil, errors.Errorf("case %v does not exist", name)
}

// GetGRPCCase returns the gRPC case config.
func (c *Coordinator) GetGRPCCase(name string) (*Config, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if controller, ok := c.grpc[name]; ok {
		return controller.getConfig(), nil
	}
	if controller, ok := c.directGRPC[name]; ok {
		return controller.getConfig(), nil
	}
	return nil, errors.Errorf("case %v does not exist", name)
}

// GetEtcdCase returns the etcd case config.
func (c *Coordinator) GetEtcdCase(name string) (*Config, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if controller, ok := c.etcd[name]; ok {
		return controller.getConfig(), nil
	}
	return nil, errors.Errorf("case %v does not exist", name)
}

// GetAllHTTPCases returns the all HTTP case configs.
func (c *Coordinator) GetAllHTTPCases() map[string]*Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ret := make(map[string]*Config)
	for name, c := range c.http {
		ret[name] = c.getConfig()
	}
	return ret
}

// GetAllGRPCCases returns the all gRPC case configs.
func (c *Coordinator) GetAllGRPCCases() map[string]*Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ret := make(map[string]*Config)
	for name, c := range c.grpc {
		ret[name] = c.getConfig()
	}
	for name, c := range c.directGRPC {
		ret[name] = c.getConfig()
	}
	return ret
}

// GetAllEtcdCases returns the all etcd case configs.
func (c *Coordinator) GetAllEtcdCases() map[string]*Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ret := make(map[string]*Config)
	for name, c := range c.etcd {
		ret[name] = c.getConfig()
	}
	return ret
}

// SetHTTPCase sets the config for the specific case.
func (c *Coordinator) SetHTTPCase(name string, cfg *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fn, ok := HTTPCaseFnMap[name]; ok {
		var controller *httpController
		if controller, ok = c.http[name]; !ok {
			controller = newHTTPController(c.ctx, c.httpClients, fn)
			c.http[name] = controller
		}
		controller.stop()
		applyCaseConfig(controller.HTTPCase, cfg)
		controller.run()
	} else {
		return errors.Errorf("HTTP case %s not implemented", name)
	}
	return nil
}

// SetGRPCCase sets the config for the specific case.
func (c *Coordinator) SetGRPCCase(name string, cfg *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fn, ok := GRPCCaseFnMap[name]; ok {
		var controller *gRPCController
		if controller, ok = c.grpc[name]; !ok {
			controller = newGRPCController(c.ctx, c.gRPCClients, fn)
			c.grpc[name] = controller
		}
		controller.stop()
		applyCaseConfig(controller.GRPCCase, cfg)
		controller.run()
	} else if fn, ok := DirectGRPCCaseFnMap[name]; ok {
		var controller *directGRPCController
		if controller, ok = c.directGRPC[name]; !ok {
			controller = newDirectGRPCController(c.ctx, c.gRPCClients, fn)
			c.directGRPC[name] = controller
		}
		controller.stop()
		applyCaseConfig(controller.DirectGRPCCase, cfg)
		controller.run()
	} else {
		return errors.Errorf("gRPC case %s not implemented", name)
	}
	return nil
}

// SetEtcdCase sets the config for the specific case.
func (c *Coordinator) SetEtcdCase(name string, cfg *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fn, ok := EtcdCaseFnMap[name]; ok {
		var controller *etcdController
		if controller, ok = c.etcd[name]; !ok {
			controller = newEtcdController(c.ctx, c.etcdClients, fn)
			c.etcd[name] = controller
		}
		controller.stop()
		applyCaseConfig(controller.EtcdCase, cfg)
		controller.run()
	} else {
		return errors.Errorf("etcd case %s not implemented", name)
	}
	return nil
}

type httpController struct {
	HTTPCase
	clients []pdHttp.Client
	pctx    context.Context

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newHTTPController(ctx context.Context, clis []pdHttp.Client, fn HTTPCreateFn) *httpController {
	c := &httpController{
		pctx:     ctx,
		clients:  clis,
		HTTPCase: fn(),
	}
	return c
}

// run tries to run the HTTP api bench.
func (c *httpController) run() {
	if c.getQPS() <= 0 || c.cancel != nil {
		return
	}
	c.ctx, c.cancel = context.WithCancel(c.pctx)
	qps := c.getQPS()
	burst := c.getBurst()
	cliNum := int64(len(c.clients))
	tt := time.Duration(base*burst*cliNum/qps) * time.Microsecond
	log.Info("begin to run http case", zap.String("case", c.getName()), zap.Int64("qps", qps), zap.Int64("burst", burst), zap.Duration("interval", tt))
	for _, hCli := range c.clients {
		c.wg.Add(1)
		go func(hCli pdHttp.Client) {
			defer c.wg.Done()
			c.wg.Add(int(burst))
			for range burst {
				go func() {
					defer c.wg.Done()
					ticker := time.NewTicker(tt)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							err := c.do(c.ctx, hCli)
							if err != nil {
								log.Error("meet error when doing HTTP request", zap.String("case", c.getName()), zap.Error(err))
							}
						case <-c.ctx.Done():
							log.Info("got signal to exit running HTTP case")
							return
						}
					}
				}()
			}
		}(hCli)
	}
}

// stop stops the HTTP api bench.
func (c *httpController) stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.wg.Wait()
}

type gRPCController struct {
	GRPCCase
	clients []pd.Client
	pctx    context.Context

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

func newGRPCController(ctx context.Context, clis []pd.Client, fn GRPCCreateFn) *gRPCController {
	c := &gRPCController{
		pctx:     ctx,
		clients:  clis,
		GRPCCase: fn(),
	}
	return c
}

// run tries to run the gRPC api bench.
func (c *gRPCController) run() {
	if c.getQPS() <= 0 || c.cancel != nil {
		return
	}
	c.ctx, c.cancel = context.WithCancel(c.pctx)
	qps := c.getQPS()
	burst := c.getBurst()
	cliNum := int64(len(c.clients))
	tt := time.Duration(base*burst*cliNum/qps) * time.Microsecond
	log.Info("begin to run gRPC case", zap.String("case", c.getName()), zap.Int64("qps", qps), zap.Int64("burst", burst), zap.Duration("interval", tt))
	for _, cli := range c.clients {
		c.wg.Add(1)
		go func(cli pd.Client) {
			defer c.wg.Done()
			c.wg.Add(int(burst))
			for range burst {
				go func() {
					defer c.wg.Done()
					ticker := time.NewTicker(tt)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							err := c.unary(c.ctx, cli)
							if err != nil {
								log.Error("meet error when doing gRPC request", zap.String("case", c.getName()), zap.Error(err))
							}
						case <-c.ctx.Done():
							log.Info("got signal to exit running gRPC case")
							return
						}
					}
				}()
			}
		}(cli)
	}
}

// stop stops the gRPC api bench.
func (c *gRPCController) stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.wg.Wait()
}

type directGRPCController struct {
	DirectGRPCCase
	clients []pd.Client
	pctx    context.Context

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

func newDirectGRPCController(ctx context.Context, clis []pd.Client, fn DirectGRPCCreateFn) *directGRPCController {
	return &directGRPCController{
		pctx:           ctx,
		clients:        clis,
		DirectGRPCCase: fn(),
	}
}

func (c *directGRPCController) run() {
	if c.getQPS() <= 0 || c.cancel != nil {
		return
	}
	c.ctx, c.cancel = context.WithCancel(c.pctx)
	qps := c.getQPS()
	burst := c.getBurst()
	cliNum := int64(len(c.clients))
	tt := time.Duration(base*burst*cliNum/qps) * time.Microsecond
	log.Info("begin to run direct gRPC case", zap.String("case", c.getName()), zap.Int64("qps", qps), zap.Int64("burst", burst), zap.Duration("interval", tt))
	for _, cli := range c.clients {
		c.wg.Add(1)
		go func(cli pd.Client) {
			defer c.wg.Done()
			c.wg.Add(int(burst))
			for range burst {
				go func() {
					defer c.wg.Done()
					ticker := time.NewTicker(tt)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							pdClient, healthClient, err := buildDirectClients(cli)
							if err != nil {
								log.Error("create direct gRPC client failed", zap.String("case", c.getName()), zap.Error(err))
								continue
							}
							err = c.unaryDirect(c.ctx, pdClient, healthClient)
							if err != nil {
								log.Error("meet error when doing direct gRPC request", zap.String("case", c.getName()), zap.Error(err))
							}
						case <-c.ctx.Done():
							log.Info("got signal to exit running direct gRPC case")
							return
						}
					}
				}()
			}
		}(cli)
	}
}

func (c *directGRPCController) stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.wg.Wait()
}

func buildDirectClients(cli pd.Client) (pdpb.PDClient, healthpb.HealthClient, error) {
	sd := cli.GetServiceDiscovery()
	conn := sd.GetServingEndpointClientConn()
	if conn == nil {
		var err error
		conn, err = sd.GetOrCreateGRPCConn(sd.GetServingURL())
		if err != nil {
			return nil, nil, err
		}
	}
	return pdpb.NewPDClient(conn), healthpb.NewHealthClient(conn), nil
}

type etcdController struct {
	EtcdCase
	clients []*clientv3.Client
	pctx    context.Context

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

func newEtcdController(ctx context.Context, clis []*clientv3.Client, fn EtcdCreateFn) *etcdController {
	c := &etcdController{
		pctx:     ctx,
		clients:  clis,
		EtcdCase: fn(),
	}
	return c
}

// run tries to run the gRPC api bench.
func (c *etcdController) run() {
	if c.getQPS() <= 0 || c.cancel != nil {
		return
	}
	c.ctx, c.cancel = context.WithCancel(c.pctx)
	qps := c.getQPS()
	burst := c.getBurst()
	cliNum := int64(len(c.clients))
	tt := time.Duration(base*burst*cliNum/qps) * time.Microsecond
	log.Info("begin to run etcd case", zap.String("case", c.getName()), zap.Int64("qps", qps), zap.Int64("burst", burst), zap.Duration("interval", tt))
	err := c.init(c.ctx, c.clients[0])
	if err != nil {
		log.Error("init error", zap.String("case", c.getName()), zap.Error(err))
		return
	}
	for _, cli := range c.clients {
		c.wg.Add(1)
		go func(cli *clientv3.Client) {
			defer c.wg.Done()
			c.wg.Add(int(burst))
			for range burst {
				go func() {
					defer c.wg.Done()
					ticker := time.NewTicker(tt)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							err := c.unary(c.ctx, cli)
							if err != nil {
								log.Error("meet error when doing etcd request", zap.String("case", c.getName()), zap.Error(err))
							}
						case <-c.ctx.Done():
							log.Info("got signal to exit running etcd case")
							return
						}
					}
				}()
			}
		}(cli)
	}
}

// stop stops the etcd api bench.
func (c *etcdController) stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.wg.Wait()
}
