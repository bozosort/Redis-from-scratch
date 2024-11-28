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

func MessageHandler(message RESP_Parser.RESPValue, conn net.Conn, RedisInfo *RedisInfo) {
	if message.Type != "Array" {
		return
	}

	cmd := message.Value.([]RESP_Parser.RESPValue)[0].Value.(string)

	RedisStore := Store.GetRedisStore()

	switch cmd {
	case "PING":
		if RedisInfo.replicaof == "none" {
			conn.Write([]byte("$4\r\nPONG\r\n"))
		}

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
			conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n" + RESP_Parser.SerializeRESP(RESP_Parser.RESPValue{"BulkString", strconv.Itoa(RedisInfo.ack_counter)})))
		}
	case "PSYNC":
		conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
		emptyRDB, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
		conn.Write([]byte("$" + strconv.Itoa(len(emptyRDB)) + "\r\n"))
		conn.Write(emptyRDB)
	case "WAIT":
		acks := handlewait(message, RedisInfo)
		fmt.Println("acks:", acks)
		conn.Write([]byte(":" + strconv.Itoa(acks) + "\r\n"))
	}

}

func propogate(message RESP_Parser.RESPValue, RedisInfo *RedisInfo) {
	str := RESP_Parser.SerializeRESP(message)
	for _, conn := range RedisInfo.conns {
		conn.Write([]byte(str))
	}
}

func handlewait(message RESP_Parser.RESPValue, RedisInfo *RedisInfo) int {
	if len(RedisInfo.conns) == 0 {
		return 0
	}

	numreplicas, _ := strconv.Atoi(message.Value.([]RESP_Parser.RESPValue)[1].Value.(string))
	if numreplicas > len(RedisInfo.conns) {
		numreplicas = len(RedisInfo.conns)
	}
	timeout, _ := strconv.Atoi(message.Value.([]RESP_Parser.RESPValue)[2].Value.(string))

	now := time.Now()
	acks := 0
	for _, conn := range RedisInfo.conns {
		conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
		fmt.Println("Wrote to", conn)
		go concurReadWait(&acks, conn)
	}
	for {
		if int(time.Since(now).Milliseconds()) > timeout || acks >= numreplicas {
			return acks
		}
	}
}

func concurReadWait(acks *int, conn net.Conn) {
	buf := make([]byte, 1024)
	for {
		conn.Read(buf)

		if string(buf[:37]) == "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n" {
			*acks += 1
			return
		}
	}
}
