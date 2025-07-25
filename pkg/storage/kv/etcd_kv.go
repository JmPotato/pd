// Copyright 2016 TiKV Project Authors.
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

package kv

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

const (
	requestTimeout  = 10 * time.Second
	slowRequestTime = time.Second
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	txnFailedCounter       = txnCounter.WithLabelValues("failed")
	txnSuccessCounter      = txnCounter.WithLabelValues("success")
	txnFailedDurationHist  = txnDuration.WithLabelValues("failed")
	txnSuccessDurationHist = txnDuration.WithLabelValues("success")
)

type etcdKVBase struct {
	client *clientv3.Client
}

// NewEtcdKVBase creates a new etcd kv.
func NewEtcdKVBase(client *clientv3.Client) *etcdKVBase {
	return &etcdKVBase{client: client}
}

// Load loads the value of the key from etcd.
func (kv *etcdKVBase) Load(key string) (string, error) {
	resp, err := etcdutil.EtcdKVGet(kv.client, key)
	if err != nil {
		return "", err
	}
	if n := len(resp.Kvs); n == 0 {
		return "", nil
	} else if n > 1 {
		return "", errs.ErrEtcdKVGetResponse.GenWithStackByArgs(resp.Kvs)
	}
	return string(resp.Kvs[0].Value), nil
}

// LoadRange loads a range of keys [key, endKey) from etcd.
func (kv *etcdKVBase) LoadRange(key, endKey string, limit int) (keys, values []string, err error) {
	var OpOption []clientv3.OpOption
	// If endKey is "\x00", it means to scan with prefix.
	// If the key is empty and endKey is "\x00", it means to scan all keys.
	if endKey == "\x00" {
		OpOption = append(OpOption, clientv3.WithPrefix())
	} else {
		OpOption = append(OpOption, clientv3.WithRange(endKey))
	}

	OpOption = append(OpOption, clientv3.WithLimit(int64(limit)))
	resp, err := etcdutil.EtcdKVGet(kv.client, key, OpOption...)
	if err != nil {
		return nil, nil, err
	}
	keys = make([]string, 0, len(resp.Kvs))
	values = make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		keys = append(keys, string(item.Key))
		values = append(values, string(item.Value))
	}
	return keys, values, nil
}

// Save puts a key-value pair to etcd.
func (kv *etcdKVBase) Save(key, value string) error {
	failpoint.Inject("etcdSaveFailed", func() {
		failpoint.Return(errors.New("save failed"))
	})

	txn := NewSlowLogTxn(kv.client)
	resp, err := txn.Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		e := errs.ErrEtcdKVPut.Wrap(err).GenWithStackByCause()
		log.Error("save to etcd meet error", zap.String("key", key), zap.String("value", value), errs.ZapError(e))
		return e
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

// Remove removes the key from etcd.
func (kv *etcdKVBase) Remove(key string) error {
	txn := NewSlowLogTxn(kv.client)
	resp, err := txn.Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		err = errs.ErrEtcdKVDelete.Wrap(err).GenWithStackByCause()
		log.Error("remove from etcd meet error", zap.String("key", key), errs.ZapError(err))
		return err
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

// CreateRawTxn creates a transaction that provides interface in if-then-else pattern.
func (kv *etcdKVBase) CreateRawTxn() RawTxn {
	return &rawTxnWrapper{
		inner: NewSlowLogTxn(kv.client),
	}
}

// SlowLogTxn wraps etcd transaction and log slow one.
type SlowLogTxn struct {
	clientv3.Txn
	cancel context.CancelFunc
}

// NewSlowLogTxn create a SlowLogTxn.
func NewSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	ctx, cancel := context.WithTimeout(client.Ctx(), requestTimeout)
	return &SlowLogTxn{
		Txn:    client.Txn(ctx),
		cancel: cancel,
	}
}

// If takes a list of comparison. If all comparisons passed in succeed,
// the operations passed into Then() will be executed. Or the operations
// passed into Else() will be executed.
func (t *SlowLogTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	t.Txn = t.Txn.If(cs...)
	return t
}

// Then takes a list of operations. The Ops list will be executed, if the
// comparisons passed in If() succeed.
func (t *SlowLogTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.Txn = t.Txn.Then(ops...)
	return t
}

// Commit implements Txn Commit interface.
func (t *SlowLogTxn) Commit() (*clientv3.TxnResponse, error) {
	start := time.Now()
	resp, err := t.Txn.Commit()
	t.cancel()

	cost := time.Since(start)
	if cost > slowRequestTime {
		log.Warn("txn runs too slow",
			zap.Reflect("response", resp),
			zap.Duration("cost", cost),
			errs.ZapError(err))
	}

	if err != nil {
		txnFailedCounter.Inc()
		txnFailedDurationHist.Observe(cost.Seconds())
	} else {
		txnSuccessCounter.Inc()
		txnSuccessDurationHist.Observe(cost.Seconds())
	}

	return resp, errors.WithStack(err)
}

// etcdTxn is used to record user's action during RunInTxn,
// It stores modification in operations to apply as a single transaction during commit.
// All load/loadRange result will be stored in conditions.
// Transaction commit will be successful only if all conditions are met,
// aka, no other transaction has modified values loaded during current transaction.
type etcdTxn struct {
	kv         *etcdKVBase
	ctx        context.Context
	conditions []clientv3.Cmp
	operations []clientv3.Op
}

// RunInTxn runs user provided function f in a transaction.
func (kv *etcdKVBase) RunInTxn(ctx context.Context, f func(txn Txn) error) error {
	txn := &etcdTxn{
		kv:  kv,
		ctx: ctx,
	}
	err := f(txn)
	if err != nil {
		return err
	}
	return txn.commit()
}

// Save puts a put operation into operations.
// Note that save result are not immediately observable before current transaction commit.
func (txn *etcdTxn) Save(key, value string) error {
	operation := clientv3.OpPut(key, value)
	txn.operations = append(txn.operations, operation)

	return nil
}

// Remove puts a delete operation into operations.
func (txn *etcdTxn) Remove(key string) error {
	operation := clientv3.OpDelete(key)
	txn.operations = append(txn.operations, operation)
	return nil
}

// Load loads the target value from etcd and puts a comparator into conditions.
func (txn *etcdTxn) Load(key string) (string, error) {
	resp, err := etcdutil.EtcdKVGet(txn.kv.client, key)
	if err != nil {
		return "", err
	}
	var condition clientv3.Cmp
	var value string
	switch respLen := len(resp.Kvs); respLen {
	case 0:
		// If target key does not contain a value, pin the CreateRevision of the key to 0.
		// Returned value should be empty string.
		value = ""
		condition = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	case 1:
		// If target key has value, must make sure it stays the same at the time of commit.
		value = string(resp.Kvs[0].Value)
		condition = clientv3.Compare(clientv3.Value(key), "=", value)
	default:
		// If response contains multiple kvs, error occurred.
		return "", errs.ErrEtcdKVGetResponse.GenWithStackByArgs(resp.Kvs)
	}
	// Append the check condition to transaction.
	txn.conditions = append(txn.conditions, condition)
	return value, nil
}

// LoadRange loads the target range from etcd,
// Then for each value loaded, it puts a comparator into conditions.
func (txn *etcdTxn) LoadRange(key, endKey string, limit int) (keys []string, values []string, err error) {
	keys, values, err = txn.kv.LoadRange(key, endKey, limit)
	// If LoadRange failed, preserve the failure behavior of base LoadRange.
	if err != nil {
		return keys, values, err
	}
	// If LoadRange successful, must make sure values stay the same before commit.
	for i := range keys {
		condition := clientv3.Compare(clientv3.Value(keys[i]), "=", values[i])
		txn.conditions = append(txn.conditions, condition)
	}
	return keys, values, err
}

// commit perform the operations on etcd, with pre-condition that values observed by user have not been changed.
func (txn *etcdTxn) commit() error {
	// Using slowLogTxn to commit transaction.
	slowLogTxn := NewSlowLogTxn(txn.kv.client)
	slowLogTxn.If(txn.conditions...)
	slowLogTxn.Then(txn.operations...)
	resp, err := slowLogTxn.Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

type rawTxnWrapper struct {
	inner clientv3.Txn
}

// If implements RawTxn interface for adding conditions to the transaction.
func (l *rawTxnWrapper) If(conditions ...RawTxnCondition) RawTxn {
	cmpList := make([]clientv3.Cmp, 0, len(conditions))
	for _, c := range conditions {
		switch c.CmpType {
		case RawTxnCmpExists:
			cmpList = append(cmpList, clientv3.Compare(clientv3.CreateRevision(c.Key), ">", 0))
		case RawTxnCmpNotExists:
			cmpList = append(cmpList, clientv3.Compare(clientv3.CreateRevision(c.Key), "=", 0))
		default:
			var cmpOp string
			switch c.CmpType {
			case RawTxnCmpEqual:
				cmpOp = "="
			case RawTxnCmpNotEqual:
				cmpOp = "!="
			case RawTxnCmpGreater:
				cmpOp = ">"
			case RawTxnCmpLess:
				cmpOp = "<"
			default:
				panic(fmt.Sprintf("unknown cmp type %v", c.CmpType))
			}
			cmpList = append(cmpList, clientv3.Compare(clientv3.Value(c.Key), cmpOp, c.Value))
		}
	}
	l.inner = l.inner.If(cmpList...)
	return l
}

func convertOps(ops []RawTxnOp) []clientv3.Op {
	opsList := make([]clientv3.Op, 0, len(ops))
	for _, op := range ops {
		switch op.OpType {
		case RawTxnOpPut:
			opsList = append(opsList, clientv3.OpPut(op.Key, op.Value))
		case RawTxnOpDelete:
			opsList = append(opsList, clientv3.OpDelete(op.Key))
		case RawTxnOpGet:
			opsList = append(opsList, clientv3.OpGet(op.Key))
		case RawTxnOpGetRange:
			if op.EndKey == "\x00" {
				opsList = append(opsList, clientv3.OpGet(op.Key, clientv3.WithPrefix(), clientv3.WithLimit(int64(op.Limit))))
			} else {
				opsList = append(opsList, clientv3.OpGet(op.Key, clientv3.WithRange(op.EndKey), clientv3.WithLimit(int64(op.Limit))))
			}
		default:
			panic(fmt.Sprintf("unknown op type %v", op.OpType))
		}
	}
	return opsList
}

// Then implements RawTxn interface for adding operations that need to be executed when the condition passes to
// the transaction.
func (l *rawTxnWrapper) Then(ops ...RawTxnOp) RawTxn {
	convertedOps := convertOps(ops)
	l.inner = l.inner.Then(convertedOps...)
	return l
}

// Else implements RawTxn interface for adding operations that need to be executed when the condition doesn't pass
// to the transaction.
func (l *rawTxnWrapper) Else(ops ...RawTxnOp) RawTxn {
	convertedOps := convertOps(ops)
	l.inner = l.inner.Else(convertedOps...)
	return l
}

// Commit implements RawTxn interface for committing the transaction.
func (l *rawTxnWrapper) Commit() (RawTxnResponse, error) {
	resp, err := l.inner.Commit()
	if err != nil {
		return RawTxnResponse{}, err
	}
	items := make([]RawTxnResponseItem, 0, len(resp.Responses))
	for i, rpcRespItem := range resp.Responses {
		var respItem RawTxnResponseItem
		if put := rpcRespItem.GetResponsePut(); put != nil {
			// Put and delete operations of etcd's transaction won't return any previous data. Skip handling it.
			respItem = RawTxnResponseItem{}
		} else if del := rpcRespItem.GetResponseDeleteRange(); del != nil {
			// Put and delete operations of etcd's transaction won't return any previous data. Skip handling it.
			respItem = RawTxnResponseItem{}
		} else if rangeResp := rpcRespItem.GetResponseRange(); rangeResp != nil {
			kvs := make([]KeyValuePair, 0, len(rangeResp.Kvs))
			for _, kv := range rangeResp.Kvs {
				kvs = append(kvs, KeyValuePair{
					Key:   string(kv.Key),
					Value: string(kv.Value),
				})
			}
			respItem = RawTxnResponseItem{
				KeyValuePairs: kvs,
			}
		} else {
			return RawTxnResponse{}, errs.ErrEtcdTxnResponse.GenWithStackByArgs(
				fmt.Sprintf("succeeded: %v, index: %v, response: %v", resp.Succeeded, i, rpcRespItem),
			)
		}
		items = append(items, respItem)
	}
	return RawTxnResponse{
		Succeeded: resp.Succeeded,
		Responses: items,
	}, nil
}
