package etcd

import (
	"context"

	"github.com/wwq1988/kv/codec"
	"go.etcd.io/etcd/clientv3"
)

// Config Config
type Config struct {
	Endpoints []string
}

// KV KV
type KV struct {
	config *Config
	codec  codec.Codec
	client *clientv3.Client
}

// New New
func New(config *Config, codec codec.Codec) *KV {
	client, err := clientv3.New(clientv3.Config{Endpoints: config.Endpoints})
	if err != nil {
		panic(err)
	}
	return &KV{
		client: client,
		config: config,
		codec:  codec,
	}
}

// Put Put
func (kv *KV) Put(ctx context.Context, key string, in interface{}) error {
	encodeResult, err := kv.codec.Encode(in)
	if err != nil {
		return err
	}

	_, err = kv.client.KV.Put(ctx, key, string(encodeResult))
	return err
}

// CAS CAS
func (kv *KV) CAS(ctx context.Context, key string, casFunc func(interface{}) (interface{}, error)) (bool, error) {
	resp, err := kv.client.KV.Get(ctx, key)
	if err != nil {
		return false, nil
	}

	var value []byte
	var revision int64
	if len(resp.Kvs) != 0 {
		value = resp.Kvs[0].Value
		revision = resp.Kvs[0].Version
	}
	decodeResult, err := kv.codec.Decode(value)
	if err != nil {
		return false, err
	}
	casResult, err := casFunc(decodeResult)
	if err != nil {
		return false, err
	}
	encodeResult, err := kv.codec.Encode(casResult)
	if err != nil {
		return false, err
	}
	casResp, err := kv.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", revision)).
		Then(clientv3.OpPut(key, string(encodeResult))).
		Commit()

	if err != nil {
		return false, err
	}
	return casResp.Succeeded, nil
}

// WatchKey WatchKey
func (kv *KV) WatchKey(ctx context.Context, key string, watchFunc func(interface{})) {

	for {
		resp, err := kv.client.KV.Get(ctx, key)
		if err != nil {
			continue
		}

		if len(resp.Kvs) == 0 {
			continue
		}

		out, err := kv.codec.Decode(resp.Kvs[0].Value)
		if err != nil {
			continue
		}
		watchFunc(out)
	}
}

// WatchPrefix WatchPrefix
func (kv *KV) WatchPrefix(ctx context.Context, prefix string, watchFunc func(string, interface{})) {
	for {
		resp, err := kv.client.KV.Get(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			continue
		}

		for _, each := range resp.Kvs {
			out, err := kv.codec.Decode(each.Value)
			if err != nil {
				continue
			}
			watchFunc(string(each.Key), out)
		}
	}
}

// List List
func (kv *KV) List(ctx context.Context, prefix string) ([]string, error) {
	resp, err := kv.client.KV.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(resp.Kvs))
	for _, each := range resp.Kvs {
		keys = append(keys, string(each.Key))
	}
	return keys, nil
}

// Get Get
func (kv *KV) Get(ctx context.Context, key string) (interface{}, error) {
	resp, err := kv.client.KV.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return kv.codec.Decode(resp.Kvs[0].Value)
}

// GetRaw GetRaw
func (kv *KV) GetRaw(ctx context.Context, key string) ([]byte, error) {
	resp, err := kv.client.KV.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

// Delete Delete
func (kv *KV) Delete(ctx context.Context, key string) error {
	_, err := kv.client.KV.Delete(ctx, key)
	return err
}
