package rmb

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

type BackendMock struct {
	lists map[string][]string
	dicts map[string]map[string]string
	ints  map[string]int64
}

func NewBackendMock() BackendMock {
	r := BackendMock{
		lists: make(map[string][]string),
		dicts: make(map[string]map[string]string),
		ints:  make(map[string]int64),
	}
	return r
}

func (r BackendMock) HGet(ctx context.Context, key string, field string) (string, error) {
	i, ok := r.dicts[key]
	if ok == false {
		return "", errors.New("couldn't find key")
	}
	v, ok := i[field]
	if ok == false {
		return "", errors.New("field not found in key")
	}
	return v, nil
}

func (r BackendMock) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return nil, nil
}

func (r BackendMock) HDel(ctx context.Context, key string, field string) (int64, error) {
	return 0, nil
}

func (r BackendMock) LPush(ctx context.Context, key string, value []byte) (int64, error) {
	return 0, nil
}

func (r BackendMock) BLPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return nil, nil
}

func (r BackendMock) Incr(ctx context.Context, key string) (int64, error) {
	return 0, nil
}

func (r BackendMock) HSet(ctx context.Context, key string, field string, value []byte) (int64, error) {
	return 0, nil
}
