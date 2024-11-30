package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/RESP_Parser"
	"github.com/codecrafters-io/redis-starter-go/app/Store"
)

func MessageHandler(message RESP_Parser.RESPValue, conn net.Conn, RedisInfo *RedisInfo) string {
	cmd := message.Value.([]RESP_Parser.RESPValue)[0].Value.(string)

	RedisStore := Store.GetRedisStore()
	ackCh := GetAckChannelInstance() //Initialize ackCh for Wait command

	switch cmd {
	case "PING":
		if RedisInfo.replicaof == "none" {
			return "$4\r\nPONG\r\n"
		}

	case "ECHO":
		str := message.Value.([]RESP_Parser.RESPValue)[1].Value.(string)
		return "$" + strconv.Itoa(len(str)) + "\r\n" + str + "\r\n"
	case "SET":
		key := message.Value.([]RESP_Parser.RESPValue)[1]
		value := message.Value.([]RESP_Parser.RESPValue)[2]
		if len(message.Value.([]RESP_Parser.RESPValue)) == 5 {
			arg := message.Value.([]RESP_Parser.RESPValue)[3].Value.(string)
			timestr := message.Value.([]RESP_Parser.RESPValue)[4].Value.(string)
			time, _ := strconv.Atoi(strings.TrimSuffix(timestr, "\r\n"))
			if arg == "ex" {
				RedisStore.Set(key, value, time*1000)
			} else if arg == "px" {
				RedisStore.Set(key, value, time)
			}
		} else {
			RedisStore.Set(key, value, -1)
		}
		RedisInfo.wait_write_counter++
		if RedisInfo.replicaof == "none" {
			propogate(message, RedisInfo)
			return "+OK\r\n"
		} else {
			return "Response NA"
		}
	case "GET":
		key := message.Value.([]RESP_Parser.RESPValue)[1]
		return RESP_Parser.SerializeRESP(RedisStore.Get(key))
	case "INFO":
		if RedisInfo.replicaof == "none" {
			return "$91\r\nrole:master\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n\r\n"
		} else {
			return "$90\r\nrole:slave\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n\r\n"
		}
	case "REPLCONF":
		return handleREPLCONF(message, conn, RedisInfo)
	case "PSYNC":
		emptyRDB, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
		return "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n" + "$" + strconv.Itoa(len(emptyRDB)) + "\r\n" + string(emptyRDB)

	case "WAIT":
		if RedisInfo.wait_write_counter == 0 {
			return ":" + strconv.Itoa(len(RedisInfo.conns)) + "\r\n"
		}
		go func(conns []net.Conn) {
			for _, conn := range conns {
				conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
			}
		}(RedisInfo.conns)

		timeout, _ := strconv.Atoi(message.Value.([]RESP_Parser.RESPValue)[2].Value.(string))
		acknowledged := 0
		numReplicas, _ := strconv.Atoi(message.Value.([]RESP_Parser.RESPValue)[1].Value.(string))
		timeoutCh := time.After(time.Duration(timeout) * time.Millisecond)
		for {
			select {
			case <-ackCh:
				acknowledged++
				if acknowledged >= numReplicas {
					fmt.Println("ackCh success")
					RedisInfo.wait_write_counter = 0
					return ":" + strconv.Itoa(acknowledged) + "\r\n"
				}
			case <-timeoutCh:
				fmt.Println("Trigerred timeoutCh")
				RedisInfo.wait_write_counter = 0
				return ":" + strconv.Itoa(acknowledged) + "\r\n"
			}
		}
	case "INCR":
		key := message.Value.([]RESP_Parser.RESPValue)[1]
		newVal := RedisStore.Increment(key)
		RedisInfo.wait_write_counter++

		if RedisInfo.replicaof == "none" {
			propogate(message, RedisInfo)
			fmt.Println("INCR Master:", RESP_Parser.SerializeRESP(newVal))

			return RESP_Parser.SerializeRESP(newVal)
		}
		return "Response NA"

	case "TYPE":
		key := message.Value.([]RESP_Parser.RESPValue)[1]
		value := RedisStore.Get(key)
		if value.Value == nil {
			return "+none\r\n"
		} else if value.Type == "BulkString" {
			return "+string\r\n"
		} else {
			return "+" + value.Type + "\r\n"
		}
	case "XADD":
		return handleXADD(message, conn, RedisInfo)
	}
	return "Response NA"
}

func propogate(message RESP_Parser.RESPValue, RedisInfo *RedisInfo) {
	str := RESP_Parser.SerializeRESP(message)
	for _, conn := range RedisInfo.conns {
		conn.Write([]byte(str))
	}
}

func handleREPLCONF(message RESP_Parser.RESPValue, conn net.Conn, RedisInfo *RedisInfo) string {
	switch message.Value.([]RESP_Parser.RESPValue)[1].Value {
	case "ACK":
		//		fmt.Println("Before trigerring ackCh")
		ackCh := GetAckChannelInstance()
		ackCh <- struct{}{}
		return "Response NA"
		//		fmt.Println("After trigerring ackCh")
	case "listening-port":
		RedisInfo.conns = append(RedisInfo.conns, conn)
		return "+OK\r\n"
	case "capa":
		return "+OK\r\n"
	case "GETACK":
		return "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n" + RESP_Parser.SerializeRESP(RESP_Parser.RESPValue{"BulkString", strconv.Itoa(RedisInfo.ack_counter)})
	}
	return "Response NA"
}

func handleXADD(message RESP_Parser.RESPValue, conn net.Conn, RedisInfo *RedisInfo) string {
	key := message.Value.([]RESP_Parser.RESPValue)[1]
	id := message.Value.([]RESP_Parser.RESPValue)[2]

	RedisStore := Store.GetRedisStore()
	streamData := RedisStore.Get(key)

	if validID(id, streamData) {
		KVs := RESP_Parser.RESPValue{"Array", message.Value.([]RESP_Parser.RESPValue)[3:]}
		entry := RESP_Parser.RESPValue{"Array", [2]RESP_Parser.RESPValue{id, KVs}}

		if streamData.Value == nil {
			RedisStore.Set(key, RESP_Parser.RESPValue{"stream", []RESP_Parser.RESPValue{entry}}, -1)
			return "$" + strconv.Itoa(len(id.Value.(string))) + "\r\n" + id.Value.(string) + "\r\n"
		} else {
			RedisStore.Set(key, RESP_Parser.RESPValue{"stream", append(streamData.Value.([]RESP_Parser.RESPValue), entry)}, -1)
			return "$" + strconv.Itoa(len(id.Value.(string))) + "\r\n" + id.Value.(string) + "\r\n"
		}

	} else {
		return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
	}
}

func validID(id RESP_Parser.RESPValue, streamData RESP_Parser.RESPValue) bool {

	strs := strings.Split(id.Value.(string), "-")
	var ID [2]int
	ID[0], _ = strconv.Atoi(strs[0])
	ID[1], _ = strconv.Atoi(strs[1])
	if ID[0] < 0 || ID[1] < 0 || (ID[0] == 0 && ID[1] == 0) {
		return false
	}

	if streamData.Value == nil {
		return true
	} else {
		lastEntry := streamData.Value.([]RESP_Parser.RESPValue)[len(streamData.Value.([]RESP_Parser.RESPValue))-1]
		lastIDstr := lastEntry.Value.([2]RESP_Parser.RESPValue)[0].Value.(string)
		strs = strings.Split(lastIDstr, "-")
		var lastID [2]int
		lastID[0], _ = strconv.Atoi(strs[0])
		lastID[1], _ = strconv.Atoi(strs[1])
		if ID[0] <= lastID[0] {
			return false
		} else if ID[1] <= lastID[1] {
			return false
		} else {
			return true
		}
	}
}
