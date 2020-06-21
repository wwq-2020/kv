package consul

import (
	"context"
	"net/http"

	consul "github.com/hashicorp/consul/api"
	"github.com/wwq1988/kv/codec"
)

// Config Config
type Config struct {
	Addr       string
	HTTPClient *http.Client
}

// KV KV
type KV struct {
	client *consul.Client
	config *Config
	codec  codec.Codec
}

// New New
func New(config *Config, codec codec.Codec) *KV {
	client, err := consul.NewClient(&consul.Config{
		Address:    config.Addr,
		Scheme:     "http",
		HttpClient: config.HTTPClient,
	})
	if err != nil {
		panic("err")
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
	writeOptions := consul.WriteOptions{}

	_, err = kv.client.KV().Put(&consul.KVPair{
		Key:   key,
		Value: encodeResult,
	}, writeOptions.WithContext(ctx))
	return err
}

// CAS CAS
func (kv *KV) CAS(ctx context.Context, key string, casFunc func(interface{}) (interface{}, error)) (bool, error) {
	queryOptions := consul.QueryOptions{}
	kvPair, _, err := kv.client.KV().Get(key, queryOptions.WithContext(ctx))
	if err != nil {
		return false, nil
	}

	var value []byte
	if kvPair != nil {
		value = kvPair.Value
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
	writeOptions := consul.WriteOptions{}
	ok, _, err := kv.client.KV().CAS(&consul.KVPair{
		Key:         key,
		Value:       encodeResult,
		ModifyIndex: kvPair.ModifyIndex,
	}, writeOptions.WithContext(ctx))
	if err != nil {
		return false, err
	}
	return ok, nil
}

// WatchKey WatchKey
func (kv *KV) WatchKey(ctx context.Context, key string, watchFunc func(interface{})) {
	options := &consul.QueryOptions{}

	for {
		kvp, _, err := kv.client.KV().Get(key, options.WithContext(ctx))
		if err != nil {
			continue
		}

		if kvp == nil {
			continue
		}

		out, err := kv.codec.Decode(kvp.Value)
		if err != nil {
			continue
		}
		watchFunc(out)
	}
}

// WatchPrefix WatchPrefix
func (kv *KV) WatchPrefix(ctx context.Context, prefix string, watchFunc func(string, interface{})) {
	options := &consul.QueryOptions{}
	for {
		kvPairs, _, err := kv.client.KV().List(prefix, options.WithContext(ctx))
		if err != nil {
			continue
		}

		for _, kvPair := range kvPairs {
			out, err := kv.codec.Decode(kvPair.Value)
			if err != nil {
				continue
			}
			watchFunc(kvPair.Key, out)
		}
	}
}

// List List
func (kv *KV) List(ctx context.Context, prefix string) ([]string, error) {
	options := &consul.QueryOptions{}
	kvPairs, _, err := kv.client.KV().List(prefix, options.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(kvPairs))
	for _, kvPair := range kvPairs {
		keys = append(keys, kvPair.Key)
	}
	return keys, nil
}

// Get Get
func (kv *KV) Get(ctx context.Context, key string) (interface{}, error) {
	options := &consul.QueryOptions{}
	kvPair, _, err := kv.client.KV().Get(key, options.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	if kvPair == nil {
		return nil, nil
	}
	return kv.codec.Decode(kvPair.Value)
}

// GetRaw GetRaw
func (kv *KV) GetRaw(ctx context.Context, key string) ([]byte, error) {
	options := &consul.QueryOptions{}
	kvPair, _, err := kv.client.KV().Get(key, options.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	if kvPair == nil {
		return nil, nil
	}
	return kvPair.Value, nil
}

// Delete Delete
func (kv *KV) Delete(ctx context.Context, key string) error {
	options := &consul.WriteOptions{}
	_, err := kv.client.KV().Delete(key, options.WithContext(ctx))
	return err
}
