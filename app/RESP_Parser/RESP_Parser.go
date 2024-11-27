package RESP_Parser

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// RESPValue holds the parsed value and its type
type RESPValue struct {
	Type  string
	Value interface{}
}

// DeserializeRESP parses a RESP message
func DeserializeRESP(reader *bufio.Reader) (*RESPValue, int, error) {
	//	fmt.Println("lola")

	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, 0, err
	}

	switch prefix {
	case '+': // Simple String
		line, _ := reader.ReadString('\n')
		return &RESPValue{"SimpleString", strings.TrimSuffix(line, "\r\n")}, len(line) + 1, nil
	case '-': // Error
		line, _ := reader.ReadString('\n')
		return &RESPValue{"Error", strings.TrimSuffix(line, "\r\n")}, len(line) + 1, nil
	case ':': // Integer
		line, _ := reader.ReadString('\n')
		return &RESPValue{"Integer", strings.TrimSuffix(line, "\r\n")}, len(line) + 1, nil
	case '$': // Bulk String
		line, _ := reader.ReadString('\n')
		length, _ := strconv.Atoi(strings.TrimSuffix(line, "\r\n"))
		if length == -1 {
			return &RESPValue{"BulkString", nil}, 5, nil // Null Bulk String
		}
		data := make([]byte, length+2)
		reader.Read(data)
		fmt.Println(length)
		fmt.Println("data[:length]")
		fmt.Println(string(data[:length+2]))
		fmt.Println(data[:length+2])
		return &RESPValue{"BulkString", string(data[:length])}, length + len(line) + 1, nil
	case '*': // Array
		line, _ := reader.ReadString('\n')
		length, _ := strconv.Atoi(strings.TrimSuffix(line, "\r\n"))
		if length == -1 {
			return &RESPValue{"Array", nil}, len(line) + 1, nil // Null Array
		}
		var elements []RESPValue
		nbytes := 0
		for i := 0; i < length; i++ {
			elem, n, _ := DeserializeRESP(reader)
			elements = append(elements, *elem)
			nbytes += n
		}
		return &RESPValue{"Array", elements}, nbytes + len(line) + 1, nil
	default:
		fmt.Println(prefix)
		line, _ := reader.ReadString('*')
		fmt.Println(line)
		return nil, 0, errors.New("unknown prefix")
	}
}

func SerializeRESP(message RESPValue) string {
	if message.Type == "Array" {
		values := message.Value.([]RESPValue)
		str := "*" + strconv.Itoa(len(values)) + "\r\n"
		for _, val := range values {
			str += (SerializeRESP(val))
		}
		//		str += "\r\n"
		return str
	}

	switch message.Type {
	case "SimpleString":
		return "+" + strconv.Itoa(len(message.Value.(string))) + "\r\n" + message.Value.(string) + "\r\n"
	case "Error":
		return "-" + strconv.Itoa(len(message.Value.(string))) + "\r\n" + message.Value.(string) + "\r\n"
	case "Integer":
		return ":" + strconv.Itoa(len(message.Value.(string))) + "\r\n" + message.Value.(string) + "\r\n"
	case "BulkString":
		if message.Value == nil {
			return "$-1\r\n"
		} else {
			return "$" + strconv.Itoa(len(message.Value.(string))) + "\r\n" + message.Value.(string) + "\r\n"
		}
	default:
		return "$-1\r\n"
	}
}
