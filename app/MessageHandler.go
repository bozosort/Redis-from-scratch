package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/RESP_Parser"
	"github.com/codecrafters-io/redis-starter-go/app/Store"
)

func MessageHandler(message RESP_Parser.RESPValue, conn net.Conn, RedisInfo *RedisInfo) string {
	defer log.Println("MessageHandler end")

	cmd := strings.ToUpper(message.Value.([]RESP_Parser.RESPValue)[0].Value.(string))

	RedisStore := Store.GetRedisStore()

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
		log.Println("Before For loop", conn)
		ackCh := GetAckChannelInstance() //Initialize ackCh for Wait command
		log.Println("ackCh address", ackCh)
		for {
			select {
			case <-ackCh:
				acknowledged++
				fmt.Println("ackCh triggered")
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

	case "XRANGE":
		return handleXRANGE(message, conn, RedisInfo)

	case "XREAD":
		go handleXREAD(message, conn, RedisInfo)

	case "XADD":
		go handleXADD(message, conn, RedisInfo)

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
	defer log.Println("handleREPLCONF end")
	switch message.Value.([]RESP_Parser.RESPValue)[1].Value {
	case "ACK":
		ackCh := GetAckChannelInstance()
		log.Println("Before trigerring ackCh", conn)
		log.Println("ackCh address", ackCh)
		ackCh <- struct{}{}
		log.Println("After trigerring ackCh")
		return "Response NA"
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

func handleXADD(message RESP_Parser.RESPValue, conn net.Conn, RedisInfo *RedisInfo) {
	defer log.Println("xadd terminates")
	key := message.Value.([]RESP_Parser.RESPValue)[1]
	cmdID := message.Value.([]RESP_Parser.RESPValue)[2]

	RedisStore := Store.GetRedisStore()
	streamData := RedisStore.Get(key)
	IDres, valid := validID(cmdID, streamData)

	if valid {
		KVs := RESP_Parser.RESPValue{"Array", message.Value.([]RESP_Parser.RESPValue)[3:]}
		entry := RESP_Parser.RESPValue{"Array", []RESP_Parser.RESPValue{RESP_Parser.RESPValue{"BulkString", IDres}, KVs}}
		xrlock := GetXReadChannelInstance()
		if streamData.Value == nil {
			RedisStore.Set(key, RESP_Parser.RESPValue{"stream", []RESP_Parser.RESPValue{entry}}, -1)
			fmt.Println("First Xadd start")
			xrlock.mu.Lock()
			if xrlock.XRead_Active == true {
				fmt.Println("before trigger3")
				xrlock.ReadCh <- struct{}{}
				fmt.Println("after trigger3")
			}
			xrlock.mu.Unlock()
			log.Println("First Xadd end")
			conn.Write([]byte("$" + strconv.Itoa(len(IDres)) + "\r\n" + IDres + "\r\n"))
		} else {
			RedisStore.Set(key, RESP_Parser.RESPValue{"stream", append(streamData.Value.([]RESP_Parser.RESPValue), entry)}, -1)
			xrlock.mu.Lock()
			if xrlock.XRead_Active == true {
				fmt.Println("before trigger3")
				xrlock.ReadCh <- struct{}{}
				fmt.Println("after trigger3")
			}
			xrlock.mu.Unlock()
			fmt.Println("Second Xadd end")
			conn.Write([]byte("$" + strconv.Itoa(len(IDres)) + "\r\n" + IDres + "\r\n"))
		}

	} else {
		conn.Write([]byte(IDres))
	}
}

func validID(id RESP_Parser.RESPValue, streamData RESP_Parser.RESPValue) (string, bool) {
	fmt.Println("validID func start")
	var comparisonID [2]int
	if streamData.Value == nil {
		//Since no value is set for stream, comparisonID is set to "identity" value
		comparisonID[0] = 0
		comparisonID[1] = 0
	} else {
		lastEntry := streamData.Value.([]RESP_Parser.RESPValue)[len(streamData.Value.([]RESP_Parser.RESPValue))-1]
		lastIDstr := lastEntry.Value.([]RESP_Parser.RESPValue)[0].Value.(string)
		strsLast := strings.Split(lastIDstr, "-")
		comparisonID[0], _ = strconv.Atoi(strsLast[0])
		comparisonID[1], _ = strconv.Atoi(strsLast[1])
	}

	strs := strings.Split(id.Value.(string), "-")
	var ID [2]int

	if strs[0] == "*" {
		ID[0] = int(time.Now().UnixMilli())
		if ID[0] > comparisonID[0] {
			ID[1] = 0
		} else {
			ID[1] = comparisonID[1] + 1
		}
		return strconv.Itoa(ID[0]) + "-" + strconv.Itoa(ID[1]), true
	} else if strs[1] == "*" {
		ID[0], _ = strconv.Atoi(strs[0])
		if ID[0] > comparisonID[0] {
			ID[1] = 0
		} else {
			ID[1] = comparisonID[1] + 1
		}
		return strconv.Itoa(ID[0]) + "-" + strconv.Itoa(ID[1]), true
	} else {
		ID[0], _ = strconv.Atoi(strs[0])
		ID[1], _ = strconv.Atoi(strs[1])
	}

	if ID[0] < 0 || ID[1] < 0 || (ID[0] == 0 && ID[1] == 0) {
		return "-ERR The ID specified in XADD must be greater than 0-0\r\n", false
	} else if ID[0] < comparisonID[0] || (ID[0] == comparisonID[0] && ID[1] <= comparisonID[1]) {
		return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n", false
	} else {
		return strconv.Itoa(ID[0]) + "-" + strconv.Itoa(ID[1]), true
	}
}

func handleXRANGE(message RESP_Parser.RESPValue, conn net.Conn, RedisInfo *RedisInfo) string {
	key := message.Value.([]RESP_Parser.RESPValue)[1]
	startID := message.Value.([]RESP_Parser.RESPValue)[2]
	endID := message.Value.([]RESP_Parser.RESPValue)[3]

	RedisStore := Store.GetRedisStore()
	streamData := RedisStore.Get(key)

	startIndex := searchIndex(startID.Value.(string), streamData.Value.([]RESP_Parser.RESPValue))
	endIndex := searchIndex(endID.Value.(string), streamData.Value.([]RESP_Parser.RESPValue))

	retStr := "*" + strconv.Itoa(endIndex-startIndex+1) + "\r\n"
	for i := startIndex; i <= endIndex; i++ {
		retStr = retStr + RESP_Parser.SerializeRESP((streamData.Value.([]RESP_Parser.RESPValue)[i]))
	}
	return retStr

}

func searchIndex(id string, slice []RESP_Parser.RESPValue) int {
	//	fmt.Println("SearchIndex")
	if id == "-" {
		return 0
	} else if id == "+" {
		return len(slice) - 1
	} else if len(slice) == 1 {
		return -1 // If not found,send the previous index
	} else {
		mid := len(slice) / 2
		//		fmt.Println(mid, len(slice))
		cmpr := compareID(id, slice[mid].Value.([]RESP_Parser.RESPValue)[0].Value.(string))
		if cmpr == 1 {
			fmt.Println("mid + searchIndex(id, slice[mid:])", mid+searchIndex(id, slice[mid:]))
			return mid + searchIndex(id, slice[mid:])
		} else if cmpr == -1 {
			fmt.Println("mid - searchIndex(id, slice[:mid])", mid-searchIndex(id, slice[:mid]))
			ret := searchIndex(id, slice[:mid])
			if ret == -1 {
				return mid - 1
			}
			return mid - searchIndex(id, slice[:mid])
		} else {
			fmt.Println("mid", mid)
			return mid
		}
	}

}

func compareID(Id string, sliceId string) int {
	//	fmt.Println("compareID")
	strsId := strings.Split(Id, "-")
	strssliceId := strings.Split(sliceId, "-")
	if strsId[0] > strssliceId[0] {
		return 1
	} else if strsId[0] < strssliceId[0] {
		return -1
	}

	if len(strsId) == 2 {
		if strsId[1] > strssliceId[1] {
			return 1
		} else if strsId[1] < strssliceId[1] {
			return -1
		}
	}

	return 0
}

func handleXREAD(message RESP_Parser.RESPValue, conn net.Conn, RedisInfo *RedisInfo) {
	defer fmt.Println("xread terminates")
	arg := message.Value.([]RESP_Parser.RESPValue)[1].Value.(string)
	var cmdIndex int
	if arg == "block" {
		cmdIndex = 4
		timeout, _ := strconv.Atoi(message.Value.([]RESP_Parser.RESPValue)[2].Value.(string))
		timeCh := time.After(time.Duration(timeout) * time.Millisecond)
		if timeout == 0 {
			timeCh = nil
		}
		xrlock := GetXReadChannelInstance()
		xrlock.mu.Lock()
		log.Println("active")
		xrlock.XRead_Active = true
		xrlock.mu.Unlock()
		log.Println("Inactive, channel:", xrlock.ReadCh)
	outer:
		for {
			log.Println("reading")
			select {
			case <-xrlock.ReadCh:
				xrlock.XRead_Active = false
				break outer
			case <-timeCh:
				xrlock.XRead_Active = false
				break outer
			}
		}
	} else {
		cmdIndex = 2
	}
	RedisStore := Store.GetRedisStore()
	length := len(message.Value.([]RESP_Parser.RESPValue))
	offset := (length - cmdIndex + 1) / 2
	retStr := "*" + strconv.Itoa(offset) + "\r\n"
	for i := cmdIndex; i < cmdIndex+offset; i++ {
		key := message.Value.([]RESP_Parser.RESPValue)[i]
		streamData := RedisStore.Get(key)
		index := searchIndex(message.Value.([]RESP_Parser.RESPValue)[i+offset].Value.(string), streamData.Value.([]RESP_Parser.RESPValue))
		if index+1 <= len(streamData.Value.([]RESP_Parser.RESPValue))-1 {
			retStr = retStr + "*2\r\n$" + strconv.Itoa(len(key.Value.(string))) + "\r\n" + key.Value.(string) + "\r\n*" + strconv.Itoa(len(streamData.Value.([]RESP_Parser.RESPValue))-(index+1)) + "\r\n"
		} else {
			retStr = retStr + "$-1\r\n"
			continue
		}
		fmt.Println("index + 1", index+1)
		for j := index + 1; j <= len(streamData.Value.([]RESP_Parser.RESPValue))-1; j++ {
			retStr = retStr + RESP_Parser.SerializeRESP((streamData.Value.([]RESP_Parser.RESPValue)[j]))
		}
	}
	fmt.Println("return retStr")
	conn.Write([]byte(retStr))
	fmt.Println(retStr)
	//	return "Response NA"

}
