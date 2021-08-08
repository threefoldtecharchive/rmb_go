package rmb

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
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
	i, ok := r.dicts[key]
	if ok == false {
		return nil, errors.New("couldn't find key")
	}

	return i, nil
}

func (r BackendMock) HDel(ctx context.Context, key string, field string) (int64, error) { // int8
	i, ok := r.dicts[key]
	if ok == false {
		return 0, errors.New("couldn't find key")
	}

	_, ok = i[field]
	if ok {
		delete(i, field)
		return 1, nil
	}
	return 0, nil
}

func (r BackendMock) LPush(ctx context.Context, key string, value []byte) (int64, error) {
	list := r.lists[key]
	list = append(list, string(value))
	return 1, nil
}

func (r BackendMock) BLPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	start, now := time.Now().Unix(), time.Now().Unix()
	for now < start+int64(timeout/10000000000) { // convert from nanoseconds to seconds
		now = time.Now().Unix()
		for _, key := range keys {
			i, ok := r.lists[key]
			if ok == false {
				continue
			}
			if len(key) > 0 {
				value := i[len(i)-1]
				r.lists[key] = i[:len(i)-1] // pop the element
				return []string{key, value}, nil
			}
		}

	}
	return nil, redis.Nil
}

func (r BackendMock) Incr(ctx context.Context, key string) (int64, error) {
	i, ok := r.ints[key]
	if ok == false {
		return 0, errors.New(fmt.sprintf("couldn't find key %s", key)
	}
	r.ints[key] += 1
	return r.ints[key], nil
}

func (r BackendMock) HSet(ctx context.Context, key string, field string, value []byte) (int64, error) {
	i, ok := r.dicts[key]
	if ok == false {
		return 0, errors.New(fmt.sprintf("couldn't find key %s", key)
	}

	r.dicts[key][field] = value
	return 1, nil
}
