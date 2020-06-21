package kv

import (
	"context"

	"github.com/wwq1988/kv/codec"
	"github.com/wwq1988/kv/consul"
	"github.com/wwq1988/kv/etcd"
)

// KV KV
type KV interface {
	Put(ctx context.Context, key string, in interface{}) error
	CAS(ctx context.Context, key string, casFunc func(interface{}) (interface{}, error)) (bool, error)
	WatchKey(ctx context.Context, key string, watchFunc func(interface{}))
	WatchPrefix(ctx context.Context, prefix string, watchFunc func(string, interface{}))
	List(ctx context.Context, prefix string) ([]string, error)
	Get(ctx context.Context, key string) (interface{}, error)
	GetRaw(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

// New New
func New(config interface{}, codec codec.Codec) KV {
	var kv KV
	switch cfg := config.(type) {
	case *consul.Config:
		kv = consul.New(cfg, codec)
	case *etcd.Config:
		kv = etcd.New(cfg, codec)
	default:
		panic("unsupport")
	}
	return kv
}
