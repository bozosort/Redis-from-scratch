package main

import (
	"errors"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/RESP_Parser"
)

type Queue struct {
	elements []RESP_Parser.RESPValue
	mu       sync.Mutex
}

func (q *Queue) Enqueue(cmd RESP_Parser.RESPValue) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.elements = append(q.elements, cmd)
}

func (q *Queue) Dequeue() (RESP_Parser.RESPValue, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.elements) == 0 {
		return RESP_Parser.RESPValue{"BulkString", nil}, errors.New("empty queue")
	}

	el := q.elements[0]
	q.elements = q.elements[1:]
	return el, nil
}

func (q Queue) length() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.elements)
}
