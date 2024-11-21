package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/RESP_Parser"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

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

		reader := bufio.NewReader(strings.NewReader(string(buf[:n])))

		message, err := RESP_Parser.DeserializeRESP(reader)

		if err != nil {
			fmt.Println("Error parsing RESP:", err)
			return
		}

//		fmt.Printf("Parsed RESP: %+v\n", message.Value.([]RESP_Parser.RESPValue)[1].Value)
		cmd := message.Value.([]RESP_Parser.RESPValue)[0].Value.(string)


		if cmd=="PING" { 
			conn.Write([]byte("$" + "4" + "\r\n" + "PONG" + "\r\n"))
		} else if cmd=="ECHO" {
			str := message.Value.([]RESP_Parser.RESPValue)[1].Value.(string)
			conn.Write([]byte("$" + strconv.Itoa(len(str)) + "\r\n" + str + "\r\n"))
		}
	}
}
