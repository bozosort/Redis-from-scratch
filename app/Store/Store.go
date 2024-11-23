package Store

import(
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/RESP_Parser"
)


type RedisStoreValue struct{
	data RESP_Parser.RESPValue
	timestamp time.Time
	timeout int
}
type RedisStore struct{
	KVpairs map[RESP_Parser.RESPValue]RedisStoreValue
}

var (
	instance *RedisStore
	once sync.Once
)

func GetRedisStore() *RedisStore{
	once.Do(func() {
		instance = &RedisStore{
			KVpairs:make(map[RESP_Parser.RESPValue]RedisStoreValue),
		}
	})
	return instance
}


func (r *RedisStore) Set(key RESP_Parser.RESPValue, value RESP_Parser.RESPValue, t int){
	r.KVpairs[key] = RedisStoreValue{data: value, timestamp: time.Now(), timeout: t}
}

func (r *RedisStore) Delete(key RESP_Parser.RESPValue) (bool){
	delete(r.KVpairs, key)
	return true
}

func (r *RedisStore) Get(key RESP_Parser.RESPValue) (RESP_Parser.RESPValue){
	if r.KVpairs[key].timeout > 0 && time.Since(r.KVpairs[key].timestamp).Milliseconds() > int64(r.KVpairs[key].timeout){
		r.Delete(key)
		return RESP_Parser.RESPValue{"BulkString","$-1\r\n"}
	}
	
	value := r.KVpairs[key].data
	return value
}