package main

import (
	"container/list"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type waiter struct {
	ch   chan []byte
	done atomic.Bool
}

type queue struct {
	mu      sync.Mutex
	items   *list.List // FIFO сообщений
	waiters *list.List // FIFO ожидающих клиентов
}

func newQueue() *queue {
	return &queue{
		items:   list.New(),
		waiters: list.New(),
	}
}

var queues sync.Map

func getQueue(name string) *queue {
	if q, ok := queues.Load(name); ok {
		return q.(*queue)
	}
	q := newQueue()
	actual, _ := queues.LoadOrStore(name, q)
	return actual.(*queue)
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	qname := r.URL.Path[1:]
	msg := r.URL.Query().Get("v")
	if msg == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	q := getQueue(qname)

	q.mu.Lock()
	defer q.mu.Unlock()

	// если есть ожидающие — отдаём первому
	for e := q.waiters.Front(); e != nil; e = q.waiters.Front() {
		wr := e.Value.(*waiter)
		q.waiters.Remove(e) // Сразу убираем из списка под локом
		if wr.done.CompareAndSwap(false, true) {
			wr.ch <- []byte(msg)
			return
		}
	}

	// иначе кладём в очередь
	q.items.PushBack([]byte(msg))
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	qname := r.URL.Path[1:]
	q := getQueue(qname)

	q.mu.Lock()

	// если есть сообщение — сразу отдаём
	if e := q.items.Front(); e != nil {
		msg := e.Value.([]byte)
		q.items.Remove(e)
		q.mu.Unlock()
		_, _ = w.Write(msg)
		return
	}
	timeoutStr := r.URL.Query().Get("timeout")

	// если нет timeout → сразу 404
	if timeoutStr == "" {
		q.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		return
	}

	timeout, err := strconv.Atoi(timeoutStr)
	if err != nil || timeout <= 0 {
		q.mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeout)*time.Second)
	defer cancel()

	// создаём waiter
	wr := &waiter{ch: make(chan []byte, 1)}
	elem := q.waiters.PushBack(wr)

	q.mu.Unlock()

	select {
	case msg := <-wr.ch:
		_, _ = w.Write(msg)
	case <-ctx.Done():
		q.mu.Lock()
		if wr.done.CompareAndSwap(false, true) {
			q.waiters.Remove(elem)
		}
		q.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
	}
}

func main() {
	p := flag.String("port", "8080", "port to listen on, 80 or 443 by default")
	flag.Parse()
	port := *p
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putHandler(w, r)
		case http.MethodGet:
			getHandler(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	go backgroundCleanup()

	fmt.Println("Starting server on port", port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		fmt.Println("Error starting server:", err)
		os.Exit(1)
	}
}

func backgroundCleanup() {
	// Периодически сканируем все очереди и удаляем те, которые пустые и не имеют ожидающих
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		queues.Range(func(k, v any) bool {
			q := v.(*queue)
			q.mu.Lock()
			if q.items.Front() == nil && q.waiters.Front() == nil {
				queues.Delete(k)
			}
			q.mu.Unlock()
			return true
		})
	}
}
