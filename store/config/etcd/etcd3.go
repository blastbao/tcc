package etcd3

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"time"
)

const (
	WatchEventTypeD = "delete"
	WatchEventTypeC = "create"
	WatchEventTypeM = "modify"
)

type etcd3 struct {
	c *clientv3.Client
}

type WatchResult struct {
	KV *mvccpb.KeyValue
	OptType string
}

func (w *WatchResult) ReverseToByte() []byte {
	if w != nil{
		rst, err := json.Marshal(w)
		if err != nil{
			panic(err)
		}
		return rst
	}
	return nil
}

func NewEtcd3Client(addrs []string, timeout int, username, password string, tls *tls.Config) (*etcd3, error) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   addrs,
		DialTimeout: time.Duration(timeout),
		TLS:         tls,
		Username:    username,
		Password:    password,
	})

	if err != nil {
		return nil, err
	}

	return &etcd3{
		c: c,
	}, nil
}

func(e *etcd3) Get(ctx context.Context, key string)  ([]byte, error){
	r, err := e.c.Get(ctx, key)
	if err != nil{
		return nil, err
	}
	if len(r.Kvs) == 0{
		return nil, fmt.Errorf("key is not exist")
	}
	return r.Kvs[0].Value, nil
}

func (e *etcd3) List(ctx context.Context, prefix string) ([][]byte, error){
	var rst [][]byte
	r, err := e.c.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil{
		return nil, err
	}
	if len(r.Kvs) == 0{
		return nil, fmt.Errorf("keys is not exist")
	}

	for _, v := range r.Kvs {
		rst = append(rst, v.Value)
	}
	return rst, nil
}

func (e *etcd3) WatchTree(ctx context.Context, prefix string, stopCh <-chan struct{}) (chan []byte, error) {
	rch := e.c.Watch(ctx, prefix, clientv3.WithPrefix())
	rst := make(chan []byte, 1)
	go func() {
		defer close(rst)
		for {
			// Check if the watch was stopped by the caller
			select {
			case <-stopCh:
				return
			case wresp := <-rch:
				for _, ev := range wresp.Events {
					var optType string
					// 判断操作类型
					switch ev.Type {
					case mvccpb.DELETE:
						optType = WatchEventTypeD
					case mvccpb.PUT:
						if ev.IsCreate() {
							optType = WatchEventTypeC
						} else if ev.IsModify() {
							optType = WatchEventTypeM
						}
					}
					watchCh := &WatchResult{
						KV: ev.Kv,
						OptType: optType,
					}
					rst <- watchCh.ReverseToByte()
				}
			}

		}
	}()
	return rst, nil
}

func (e *etcd3) Close(ctx context.Context){
	e.c.Close()
}