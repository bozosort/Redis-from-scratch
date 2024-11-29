package Store

import (
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/RESP_Parser"
)

type RedisStoreValue struct {
	data      RESP_Parser.RESPValue
	timestamp time.Time
	timeout   int
}
type RedisStore struct {
	KVpairs map[RESP_Parser.RESPValue]RedisStoreValue
}

var (
	instance *RedisStore
	once     sync.Once
)

func GetRedisStore() *RedisStore {
	once.Do(func() {
		instance = &RedisStore{
			KVpairs: make(map[RESP_Parser.RESPValue]RedisStoreValue),
		}
	})
	return instance
}

func (r *RedisStore) Set(key RESP_Parser.RESPValue, value RESP_Parser.RESPValue, t int) {
	r.KVpairs[key] = RedisStoreValue{data: value, timestamp: time.Now(), timeout: t}
}

func (r *RedisStore) Delete(key RESP_Parser.RESPValue) bool {
	delete(r.KVpairs, key)
	return true
}

func (r *RedisStore) Get(key RESP_Parser.RESPValue) RESP_Parser.RESPValue {

	StoreValue, exists := r.KVpairs[key]

	if exists == false {
		return RESP_Parser.RESPValue{"BulkString", nil}
	}

	if StoreValue.timeout > 0 && time.Since(StoreValue.timestamp).Milliseconds() > int64(StoreValue.timeout) {
		r.Delete(key)
		return RESP_Parser.RESPValue{"BulkString", nil}
	}

	return StoreValue.data
}

func (r *RedisStore) Increment(key RESP_Parser.RESPValue) RESP_Parser.RESPValue {

	StoreValue, exists := r.KVpairs[key]

	if !exists {
		r.KVpairs[key] = RedisStoreValue{data: RESP_Parser.RESPValue{"Integer", "1"}, timestamp: time.Now(), timeout: -1}
		return RESP_Parser.RESPValue{"Integer", "1"}
	}

	if StoreValue.timeout > 0 && time.Since(StoreValue.timestamp).Milliseconds() > int64(StoreValue.timeout) {
		r.KVpairs[key] = RedisStoreValue{data: RESP_Parser.RESPValue{"Integer", "1"}, timestamp: time.Now(), timeout: -1}
		return RESP_Parser.RESPValue{"Integer", "1"}
	}

	temp := r.KVpairs[key]
	val, err := strconv.Atoi(temp.data.Value.(string))
	if err != nil {
		return RESP_Parser.RESPValue{"Error", "Value type is not integer"}
	}
	temp.data.Value = strconv.Itoa(val + 1)
	r.KVpairs[key] = temp
	return r.KVpairs[key].data
}
