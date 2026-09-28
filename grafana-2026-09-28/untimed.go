// untimed sends token requests shaped like TiKV background and TiFlash
// reports: flagged, carrying consumption, without per-second RU.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/pkg/caller"
)

func main() {
	group := flag.String("group", "rg_oltp", "resource group")
	keyspace := flag.Uint("keyspace", 1, "keyspace id")
	interval := flag.Duration("interval", 5*time.Second, "report interval")
	flag.Parse()
	ctx := context.Background()
	cli, err := pd.NewClientWithContext(ctx, caller.TestComponent, []string{"127.0.0.1:29379"}, pd.SecurityOption{})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	ks := &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: uint32(*keyspace)}}
	for range time.Tick(*interval) {
		for _, bg := range []bool{true, false} {
			req := &rmpb.TokenBucketRequest{
				ResourceGroupName:           *group,
				KeyspaceId:                  ks,
				IsBackground:                bg,
				IsTiflash:                   !bg,
				ConsumptionSinceLastRequest: &rmpb.Consumption{RRU: 500, WRU: 50},
			}
			_, err := cli.AcquireTokenBuckets(ctx, &rmpb.TokenBucketsRequest{Requests: []*rmpb.TokenBucketRequest{req}, TargetRequestPeriodMs: 5000, ClientUniqueId: 424242})
			if err != nil {
				log.Println("acquire:", err)
			}
		}
	}
}
