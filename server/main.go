package main

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

var (
	numRequests      int
	numRequestsMutex sync.Mutex
)

func Endpoint(w http.ResponseWriter, r *http.Request) {
	numRequestsMutex.Lock()
	numRequests++
	numRequestsMutex.Unlock()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Hello World"))
}

func Metric(w http.ResponseWriter, r *http.Request) {
	numRequestsMutex.Lock()
	metric := numRequests
	numRequests++
	numRequestsMutex.Unlock()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprint(metric)))
}

func main() {
	numRequests = 0
	r := mux.NewRouter()
	r.HandleFunc("/", Endpoint)
	r.HandleFunc("/metric", Metric)
	http.ListenAndServe(":8000", r)
}
