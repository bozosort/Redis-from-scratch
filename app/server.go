package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/RESP_Parser"
	"github.com/codecrafters-io/redis-starter-go/app/Store"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	fmt.Println("Logs from your program will appear here!")

	portPtr := flag.Int("port", 6379, "an int")
	replicaofPtr := flag.String("replicaof", "none", "a string")
	flag.Parse()

	l, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(*portPtr))
	if err != nil {
		fmt.Println("Failed to bind to port " + strconv.Itoa(*portPtr))
		os.Exit(1)
	}

	var infoData string
	if *replicaofPtr == "none" {
		infoData = "master"
	} else {
		infoData = "slave"
		go handleMasterConnection(*replicaofPtr, *portPtr)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, infoData)
	}
}

func handleMasterConnection(masterAdd string, port int) {
	conn, err := net.Dial("tcp", strings.ReplaceAll(masterAdd, " ", ":"))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer conn.Close()
	buf := make([]byte, 1024)

	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))

	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Failed to read")
		fmt.Println(err)
	}
	if string(buf[:n]) == "+PONG\r\n" {
		conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + strconv.Itoa(port) + "\r\n"))
	} else {
		fmt.Println("Failed to receive correct response, master server sent:")
		fmt.Println(string(buf[:n]))

	}

	n, err = conn.Read(buf)
	if err != nil {
		fmt.Println("Failed to read")
		fmt.Println(err)
	}
	if string(buf[:n]) == "+OK\r\n" {
		conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	} else {
		fmt.Println("Failed to receive correct response, master server sent:")
		fmt.Println(string(buf[:n]))

	}

	n, err = conn.Read(buf)
	if err != nil {
		fmt.Println("Failed to read")
		fmt.Println(err)
	}
	if string(buf[:n]) == "+OK\r\n" {
		conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	} else {
		fmt.Println("Failed to receive correct response, master server sent:")
		fmt.Println(string(buf[:n]))

	}

}

func handleConnection(conn net.Conn, infoData string) {
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Failed to read")
			fmt.Println(err)
		}

		RedisStore := Store.GetRedisStore()

		reader := bufio.NewReader(strings.NewReader(string(buf[:n])))

		message, err := RESP_Parser.DeserializeRESP(reader)

		if err != nil {
			fmt.Println("Error parsing RESP:", err)
			return
		}

		//		fmt.Printf("Parsed RESP: %+v\n", message.Value.([]RESP_Parser.RESPValue)[1].Value)
		cmd := message.Value.([]RESP_Parser.RESPValue)[0].Value.(string)

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
			conn.Write([]byte("+OK\r\n"))
		case "GET":
			key := message.Value.([]RESP_Parser.RESPValue)[1]
			conn.Write([]byte(RESP_Parser.SerializeRESP(RedisStore.Get(key))))
		case "INFO":
			if infoData == "master" {
				conn.Write([]byte("$91\r\nrole:master\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n\r\n"))
			} else {
				conn.Write([]byte("$90\r\nrole:slave\r\nmaster_repl_offset:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n\r\n"))
			}
		case "REPLCONF":
			conn.Write([]byte("+OK\r\n"))
		case "PSYNC":
			conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
			emptyRDB, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
			conn.Write([]byte("$" + string(len(emptyRDB))))
			conn.Write(emptyRDB)
		}

	}
}
