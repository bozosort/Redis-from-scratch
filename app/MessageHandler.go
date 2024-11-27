package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/RESP_Parser"
	"github.com/codecrafters-io/redis-starter-go/app/Store"
)

func MessageHandler(message RESP_Parser.RESPValue, conn net.Conn, RedisInfo *RedisInfo) {
	//	fmt.Println(1)
	if message.Type != "Array" {
		fmt.Println(message.Value)
		return
	}
	cmd := message.Value.([]RESP_Parser.RESPValue)[0].Value.(string)
	//	fmt.Println(2)

	RedisStore := Store.GetRedisStore()

	switch cmd {
	case "PING":
		conn.Write([]byte("$4\r\nPONG\r\n"))
	case "ECHO":
		str := message.Value.([]RESP_Parser.RESPValue)[1].Value.(string)
		conn.Write([]byte("$" + strconv.Itoa(len(str)) + "\r\n" + str + "\r\n"))
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
		if RedisInfo.replicaof == "none" {
			conn.Write([]byte("+OK\r\n"))
			propogate(message, RedisInfo)
		}
	case "GET":
		key := message.Value.([]RESP_Parser.RESPValue)[1]
		conn.Write([]byte(RESP_Parser.SerializeRESP(RedisStore.Get(key))))
	case "INFO":
		if RedisInfo.replicaof == "none" {
			conn.Write([]byte("$91\r\nrole:master\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n\r\n"))
		} else {
			conn.Write([]byte("$90\r\nrole:slave\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n\r\n"))
		}
	case "REPLCONF":
		if message.Value.([]RESP_Parser.RESPValue)[1].Value == "listening-port" {
			conn.Write([]byte("+OK\r\n"))
			RedisInfo.conns = append(RedisInfo.conns, conn)
		} else if message.Value.([]RESP_Parser.RESPValue)[1].Value == "capa" {
			conn.Write([]byte("+OK\r\n"))
		} else if message.Value.([]RESP_Parser.RESPValue)[1].Value == "GETACK" && message.Value.([]RESP_Parser.RESPValue)[2].Value == "*" {
			conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"))
		}
	case "PSYNC":
		conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
		emptyRDB, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
		conn.Write([]byte("$" + strconv.Itoa(len(emptyRDB)) + "\r\n"))
		conn.Write(emptyRDB)
	}

}

func propogate(message RESP_Parser.RESPValue, RedisInfo *RedisInfo) {
	str := RESP_Parser.SerializeRESP(message)
	for _, conn := range RedisInfo.conns {
		fmt.Println(str)
		fmt.Println(conn)
		fmt.Println("ale")
		conn.Write([]byte(str))
	}
}
