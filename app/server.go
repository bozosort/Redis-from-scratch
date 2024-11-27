package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
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
	RDB       RESP_Parser.RESPValue
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	portPtr := flag.Int("port", 6379, "an int")
	replicaofPtr := flag.String("replicaof", "none", "a string")

	flag.Parse()

	RedisInfo := RedisInfo{strconv.Itoa(*portPtr), *replicaofPtr, []net.Conn{}, RESP_Parser.RESPValue{"BulkString", "$-1\r\n"}}

	l, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(*portPtr))
	if err != nil {
		fmt.Println("Failed to bind to port " + strconv.Itoa(*portPtr))
		os.Exit(1)
	}

	if *replicaofPtr != "none" {
		conn, err := net.Dial("tcp", strings.ReplaceAll(*replicaofPtr, " ", ":"))
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		buf := make([]byte, 1024)
		handshake(&buf, conn, &RedisInfo)
		go handleConnection(&buf, conn, &RedisInfo)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		buf := make([]byte, 1024)
		go handleConnection(&buf, conn, &RedisInfo)

	}
}

func handleConnection(buf *[]byte, conn net.Conn, RedisInfo *RedisInfo) {
	defer conn.Close()

	for {
		nbuf, err := conn.Read(*buf)
		if err != nil {
			fmt.Println("Failed to read2")
			fmt.Println(err)
			if err == io.EOF {
				break
			}
		}
		if nbuf == 0 {
			fmt.Println("nbuf==0")
			continue // Skip if no data is received
		}

		fmt.Println("Received data:", string((*buf)[:nbuf]))

		reader := bufio.NewReader(strings.NewReader(string((*buf)[:nbuf])))

		processed := 0
		for processed < nbuf {
			message, n, err := RESP_Parser.DeserializeRESP(reader)
			if err != nil {
				fmt.Println("Error parsing RESP:", err)
				break
			}
			processed += n
			fmt.Println("Processed:", processed, "of", nbuf)
			MessageHandler(*message, conn, RedisInfo)
		}
	}
}

func handshake(buf *[]byte, conn net.Conn, RedisInfo *RedisInfo) {
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))

	n, err := conn.Read(*buf)
	if err != nil {
		fmt.Println("Failed to read1")
		fmt.Println(err)
		if err == io.EOF {
			return
		}
	}
	if string((*buf)[:n]) == "+PONG\r\n" {
		conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + RedisInfo.port + "\r\n"))
	} else {
		fmt.Println("Failed to receive correct response, master server sent:1")
		fmt.Println(string((*buf)[:n]))
		return
	}

	n, err = conn.Read(*buf)
	if err != nil {
		fmt.Println("Failed to read")
		fmt.Println(err)
		if err == io.EOF {
			return
		}
	}
	if string((*buf)[:n]) == "+OK\r\n" {
		conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	} else {
		fmt.Println("Failed to receive correct response, master server sent:2")
		fmt.Println(string((*buf)[:n]))
		return
	}

	n, err = conn.Read(*buf)
	if err != nil {
		fmt.Println("Failed to read")
		fmt.Println(err)
		if err == io.EOF {
			return
		}
	}
	if string((*buf)[:n]) == "+OK\r\n" {
		conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	} else {
		fmt.Println("Failed to receive correct response, master server sent:3")
		fmt.Println(string((*buf)[:n]))
		return
	}

	n, err = conn.Read(*buf)
	if err != nil {
		fmt.Println("Failed to read")
		fmt.Println(err)
		if err == io.EOF {
			return
		}
	}
	if string((*buf)[:11]) == "+FULLRESYNC" {
	} else {
		fmt.Println("Failed to receive correct response, master server sent:3")
		fmt.Println(string((*buf)[:n]))
		return
	}

	n, err = conn.Read(*buf)
	if err != nil {
		fmt.Println("Failed to read")
		fmt.Println(err)
		if err == io.EOF {
			return
		}
	}

	reader := bufio.NewReader(strings.NewReader(string((*buf)[:n])))

	prefix, _ := reader.ReadByte()
	// RDB is a bulk string
	if prefix == '$' {
		line, _ := reader.ReadString('\n')
		length, _ := strconv.Atoi(strings.TrimSuffix(line, "\r\n"))
		data := make([]byte, length)
		reader.Read(data)
		RedisInfo.RDB = RESP_Parser.RESPValue{"BulkString", string(data[:length])}
		fmt.Println("string(data[:length])")
		fmt.Println(string(data[:length]))

	} else {
		fmt.Println("Failed to receive correct response, master server sent:3")
		fmt.Println(string((*buf)[:n]))
		return
	}

	len := 0
	for len < n {
		message, n, err := RESP_Parser.DeserializeRESP(reader)
		if err != nil {
			fmt.Println("Error parsing RESP:", err)
			return
		}
		len += n
		MessageHandler(*message, conn, RedisInfo)
	}

}
