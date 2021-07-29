package raft

import (
	"log"
	"sync"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type EventDispatcher struct {
	mtx       sync.Mutex
	listeners []chan<- interface{}
}

func (e *EventDispatcher) Emit(data interface{}) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	for _, listener := range e.listeners {
		select {
		case listener <- data:
		default:
		}
		close(listener)
	}
	e.listeners = nil
}

func (e *EventDispatcher) Listen() <-chan interface{} {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	ch := make(chan interface{})
	e.listeners = append(e.listeners, ch)
	return ch
}
