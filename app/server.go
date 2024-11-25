package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/RESP_Parser"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type RedisInfo struct {
	port      string
	replicaof string
	conns     []net.Conn
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	portPtr := flag.Int("port", 6379, "an int")
	replicaofPtr := flag.String("replicaof", "none", "a string")

	flag.Parse()

	RedisInfo := RedisInfo{strconv.Itoa(*portPtr), *replicaofPtr, []net.Conn{}}

	l, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(*portPtr))
	if err != nil {
		fmt.Println("Failed to bind to port " + strconv.Itoa(*portPtr))
		os.Exit(1)
	}

	if *replicaofPtr == "none" {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("Error accepting connection: ", err.Error())
				os.Exit(1)
			}
			go handleConnection(conn, &RedisInfo)
		}
	} else {
		for {
			conn, err := net.Dial("tcp", strings.ReplaceAll(*replicaofPtr, " ", ":"))
			if err != nil {
				fmt.Println("Error accepting connection: ", err.Error())
				os.Exit(1)
			}
			go handleConnection(conn, &RedisInfo)
		}
	}
}

func handleConnection(conn net.Conn, RedisInfo *RedisInfo) {
	defer conn.Close()
	buf := make([]byte, 1024)

	if RedisInfo.replicaof != "none" {
		handshake(conn, &buf, RedisInfo.port)
	}

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

		MessageHandler(*message, conn, RedisInfo)
	}
}

func handshake(conn net.Conn, buf *[]byte, port string) {
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))

	n, err := conn.Read(*buf)
	if err != nil {
		fmt.Println("Failed to read")
		fmt.Println(err)
	}
	if string((*buf)[:n]) == "+PONG\r\n" {
		conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + port + "\r\n"))
	} else {
		fmt.Println("Failed to receive correct response, master server sent:")
		fmt.Println(string((*buf)[:n]))

	}

	n, err = conn.Read(*buf)
	if err != nil {
		fmt.Println("Failed to read")
		fmt.Println(err)
	}
	if string((*buf)[:n]) == "+OK\r\n" {
		conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	} else {
		fmt.Println("Failed to receive correct response, master server sent:")
		fmt.Println(string((*buf)[:n]))

	}

	n, err = conn.Read(*buf)
	if err != nil {
		fmt.Println("Failed to read")
		fmt.Println(err)
	}
	if string((*buf)[:n]) == "+OK\r\n" {
		conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	} else {
		fmt.Println("Failed to receive correct response, master server sent:")
		fmt.Println(string((*buf)[:n]))

	}
}
