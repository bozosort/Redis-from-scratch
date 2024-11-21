package RESP_Parser

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
)

// RESPValue holds the parsed value and its type
type RESPValue struct {
	Type  string
	Value interface{}
}

// DeserializeRESP parses a RESP message
func DeserializeRESP(reader *bufio.Reader) (*RESPValue, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+': // Simple String
		line, _ := reader.ReadString('\n')
		return &RESPValue{"SimpleString", strings.TrimSuffix(line, "\r\n")}, nil
	case '-': // Error
		line, _ := reader.ReadString('\n')
		return &RESPValue{"Error", strings.TrimSuffix(line, "\r\n")}, nil
	case ':': // Integer
		line, _ := reader.ReadString('\n')
		num, _ := strconv.Atoi(strings.TrimSuffix(line, "\r\n"))
		return &RESPValue{"Integer", num}, nil
	case '$': // Bulk String
		line, _ := reader.ReadString('\n')
		length, _ := strconv.Atoi(strings.TrimSuffix(line, "\r\n"))
		if length == -1 {
			return &RESPValue{"BulkString", nil}, nil // Null Bulk String
		}
		data := make([]byte, length+2)
		reader.Read(data)
		return &RESPValue{"BulkString", string(data[:length])}, nil
	case '*': // Array
		line, _ := reader.ReadString('\n')
		length, _ := strconv.Atoi(strings.TrimSuffix(line, "\r\n"))
		if length == -1 {
			return &RESPValue{"Array", nil}, nil // Null Array
		}
		var elements []RESPValue
		for i := 0; i < length; i++ {
			elem, _ := DeserializeRESP(reader)
			elements = append(elements, *elem)
		}
		return &RESPValue{"Array", elements}, nil
	default:
		return nil, errors.New("unknown prefix")
	}
}