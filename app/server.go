package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"strconv"
	"flag"

	"github.com/codecrafters-io/redis-starter-go/app/RESP_Parser"
	"github.com/codecrafters-io/redis-starter-go/app/Store"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	fmt.Println("Logs from your program will appear here!")

	portPtr := flag.Int("port", 6379, "an int")
	flag.Parse()
	fmt.Println("to bind to port " + strconv.Itoa(*portPtr))

	l, err := net.Listen("tcp", "0.0.0.0:" + strconv.Itoa(*portPtr))
	if err != nil {
		fmt.Println("Failed to bind to port " + strconv.Itoa(*portPtr))
		os.Exit(1)
	}

	fmt.Println("to bind to port " + strconv.Itoa(*portPtr))


	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Failed to read")
			fmt.Println(err)
		}
		
		RedisStore:=Store.GetRedisStore()
		
		reader := bufio.NewReader(strings.NewReader(string(buf[:n])))

		message, err := RESP_Parser.DeserializeRESP(reader)

		if err != nil {
			fmt.Println("Error parsing RESP:", err)
			return
		}

//		fmt.Printf("Parsed RESP: %+v\n", message.Value.([]RESP_Parser.RESPValue)[1].Value)
		cmd := message.Value.([]RESP_Parser.RESPValue)[0].Value.(string)

		switch cmd{
		case "PING":
			conn.Write([]byte("$4\r\nPONG\r\n"))
		case "ECHO":
			str := message.Value.([]RESP_Parser.RESPValue)[1].Value.(string)
			conn.Write([]byte("$" + strconv.Itoa(len(str)) + "\r\n" + str + "\r\n"))
		case "SET":
			key := message.Value.([]RESP_Parser.RESPValue)[1]
			value := message.Value.([]RESP_Parser.RESPValue)[2]
			if len(message.Value.([]RESP_Parser.RESPValue)) == 5{
				arg := message.Value.([]RESP_Parser.RESPValue)[3].Value.(string)
				timestr := message.Value.([]RESP_Parser.RESPValue)[4].Value.(string)
				time, _ := strconv.Atoi(strings.TrimSuffix(timestr, "\r\n"))
				if arg == "ex"{
					RedisStore.Set(key,value, time*1000)
				} else if arg == "px"{
					RedisStore.Set(key,value, time)
				}
			} else{
				RedisStore.Set(key, value, -1)
			}
			conn.Write([]byte("+OK\r\n"))
		case "GET":
			key := message.Value.([]RESP_Parser.RESPValue)[1]
			conn.Write([]byte(RESP_Parser.SerializeRESP(RedisStore.Get(key))))
		case "INFO":
			conn.Write([]byte("$11\r\nrole:master\r\n"))
		}

	}
}