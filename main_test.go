package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

func resetQueues() {
	queues = sync.Map{}
}

func TestBrokerFIFO(t *testing.T) {
	resetQueues()

	go func() {
		time.Sleep(3 * time.Second)
		for i := range 10 {
			putHandler(httptest.NewRecorder(), reqPut("/topic1?v=MSG_"+strconv.Itoa(i)))
		}
	}()

	for i := range 10 {
		w := httptest.NewRecorder()
		getHandler(w, reqGet("/topic1?timeout=5"))
		res := w.Result()
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		//fmt.Println("Response code:", w.Code)
		//fmt.Println("Response body:", string(data))
		if string(data) != "MSG_"+strconv.Itoa(i) {
			t.Fatalf("Error: expected MSG_%d, got %s", i, string(data))
		} else {
			t.Logf("OK expected MSG_%d, got %s", i, string(data))
		}
	}

}

func TestFIFO(t *testing.T) {
	resetQueues()

	putHandler(httptest.NewRecorder(), reqPut("/q?v=1"))
	putHandler(httptest.NewRecorder(), reqPut("/q?v=2"))

	w1 := httptest.NewRecorder()
	getHandler(w1, reqGet("/q"))
	res := w1.Result()
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Response1 code:", w1.Code)
	fmt.Println("Response1 body:", string(data))
	if string(data) != "1" {
		t.Fatalf("Error: expected 1, got %s", string(data))
	} else {
		t.Logf("OK expected 1, got %s", string(data))
	}

	w2 := httptest.NewRecorder()
	getHandler(w2, reqGet("/q"))
	res2 := w2.Result()
	defer res2.Body.Close()
	data2, err := io.ReadAll(res2.Body)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Response2 code:", w2.Code)
	fmt.Println("Response2 body:", string(data2))
	if string(data2) != "2" {
		t.Fatalf("Error: expected 2, got %s", string(data2))
	} else {
		t.Logf("OK expected 2, got %s", string(data2))
	}
}

func TestEmptyQueue(t *testing.T) {
	resetQueues()

	w := httptest.NewRecorder()
	getHandler(w, reqGet("/q"))

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestTimeout(t *testing.T) {
	resetQueues()

	start := time.Now()

	w := httptest.NewRecorder()
	getHandler(w, reqGet("/q?timeout=1"))

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}

	if time.Since(start) < time.Second {
		t.Fatalf("timeout didn't wait")
	}
}

func TestWaiterGetsMessage(t *testing.T) {
	resetQueues()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		w := httptest.NewRecorder()
		getHandler(w, reqGet("/q?timeout=2"))

		if body(w) != "hello" {
			panic("expected hello, got " + body(w))
		}
	}()

	time.Sleep(100 * time.Millisecond)

	putHandler(httptest.NewRecorder(), reqPut("/q?v=hello"))

	wg.Wait()
}

// func TestWaiterFIFO(t *testing.T) {
// 	resetQueues()

// 	results := make([]string, 2)
// 	var wg sync.WaitGroup
// 	wg.Add(2)

// 	go func() {
// 		defer wg.Done()
// 		w := httptest.NewRecorder()
// 		getHandler(w, reqGet("/q?timeout=2"))
// 		results[0] = body(w)
// 	}()

// 	go func() {
// 		defer wg.Done()
// 		w := httptest.NewRecorder()
// 		getHandler(w, reqGet("/q?timeout=2"))
// 		results[1] = body(w)
// 	}()

// 	time.Sleep(100 * time.Millisecond)

// 	putHandler(httptest.NewRecorder(), reqPut("/q?v=first"))
// 	putHandler(httptest.NewRecorder(), reqPut("/q?v=second"))

// 	wg.Wait()

// 	if results[0] != "first" || results[1] != "second" {
// 		t.Fatalf("FIFO violated: %+v", results)
// 	}
// }

func reqPut(url string) *http.Request {
	r := httptest.NewRequest(http.MethodPut, url, nil)
	return r
}

func reqGet(url string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, url, nil)
	return r
}

func body(w *httptest.ResponseRecorder) string {
	res := w.Result()
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return string(b)
}
func TestWaiterFIFO(t *testing.T) {
	resetQueues()

	results := make([]string, 2)

	started1 := make(chan struct{})
	started2 := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		close(started1)

		w := httptest.NewRecorder()
		getHandler(w, reqGet("/q?timeout=2"))

		results[0] = body(w)
	}()

	<-started1
	time.Sleep(50 * time.Millisecond)

	go func() {
		defer wg.Done()

		close(started2)

		w := httptest.NewRecorder()
		getHandler(w, reqGet("/q?timeout=2"))

		results[1] = body(w)
	}()

	<-started2
	time.Sleep(50 * time.Millisecond)

	putHandler(httptest.NewRecorder(), reqPut("/q?v=first"))
	putHandler(httptest.NewRecorder(), reqPut("/q?v=second"))

	wg.Wait()

	if results[0] != "first" || results[1] != "second" {
		t.Fatalf("FIFO violated: %+v", results)
	}
}