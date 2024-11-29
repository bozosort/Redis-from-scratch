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
	ackCh := GetAckChannelInstance() //Initialize ackCh for Wait command

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
		RedisInfo.wait_write_counter++
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
		handleREPLCONF(message, conn, RedisInfo)
	case "PSYNC":
		conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
		emptyRDB, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
		conn.Write([]byte("$" + strconv.Itoa(len(emptyRDB)) + "\r\n"))
		conn.Write(emptyRDB)
	case "WAIT":
		if RedisInfo.wait_write_counter == 0 {
			conn.Write([]byte(":" + strconv.Itoa(len(RedisInfo.conns)) + "\r\n"))
			return
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
					conn.Write([]byte(":" + strconv.Itoa(acknowledged) + "\r\n"))
					fmt.Println("ackCh success")
					RedisInfo.wait_write_counter = 0
					return
				}
			case <-timeoutCh:
				fmt.Println("Trigerred timeoutCh")
				conn.Write([]byte(":" + strconv.Itoa(acknowledged) + "\r\n"))
				RedisInfo.wait_write_counter = 0
				return
			}
		}
	case "INCR":
		key := message.Value.([]RESP_Parser.RESPValue)[1]
		newVal := RedisStore.Increment(key)

		if RedisInfo.replicaof == "none" {
			conn.Write([]byte(RESP_Parser.SerializeRESP(newVal)))
			propogate(message, RedisInfo)
		}
		RedisInfo.wait_write_counter++
	}

}

func propogate(message RESP_Parser.RESPValue, RedisInfo *RedisInfo) {
	str := RESP_Parser.SerializeRESP(message)
	for _, conn := range RedisInfo.conns {
		conn.Write([]byte(str))
	}
}

func handleREPLCONF(message RESP_Parser.RESPValue, conn net.Conn, RedisInfo *RedisInfo) {
	switch message.Value.([]RESP_Parser.RESPValue)[1].Value {
	case "ACK":
		//		fmt.Println("Before trigerring ackCh")
		ackCh := GetAckChannelInstance()
		ackCh <- struct{}{}
		//		fmt.Println("After trigerring ackCh")
	case "listening-port":
		conn.Write([]byte("+OK\r\n"))
		RedisInfo.conns = append(RedisInfo.conns, conn)
	case "capa":
		conn.Write([]byte("+OK\r\n"))
	case "GETACK":
		conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n" + RESP_Parser.SerializeRESP(RESP_Parser.RESPValue{"BulkString", strconv.Itoa(RedisInfo.ack_counter)})))
	}
}
