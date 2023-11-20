package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	dlts "github.com/achyuta116/big-data-projects/dlts/lib"
	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
)

var (
	broker string
	nodes  map[string]*struct {
		Up     bool
		Last   time.Time
		NodeIp string
	}
	nodesLock    sync.Mutex
	metricsStore map[string]dlts.Metrics
)

func getTestId() string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, 20)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

func Avalanche(w http.ResponseWriter, r *http.Request) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   "test_config",
	})

	testId := getTestId()
	testConfig, _ := json.Marshal(dlts.TestConfig{
		Type:         "AVALANCHE",
		TestId:       testId,
		MessageDelay: 0,
	})

	writer.WriteMessages(context.Background(), kafka.Message{
		Value: testConfig,
	})

	writer.Close()

	w.WriteHeader(http.StatusOK)
	resp, _ := json.Marshal(struct {
		TestId string
	}{testId})
	w.Write(resp)
}

func Tsunami(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	messageDelay, err := strconv.Atoi(vars["num"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   "test_config",
	})

	testId := getTestId()
	testConfig, _ := json.Marshal(dlts.TestConfig{
		Type:         "TSUNAMI",
		TestId:       testId,
		MessageDelay: messageDelay,
	})

	writer.WriteMessages(context.Background(), kafka.Message{
		Value: testConfig,
	})

	writer.Close()

	w.WriteHeader(http.StatusOK)
	resp, _ := json.Marshal(struct {
		TestId string
	}{testId})
	w.Write(resp)
}

func Metrics(w http.ResponseWriter, r *http.Request) {}
func Tests(w http.ResponseWriter, r *http.Request)   {}

func Trigger(w http.ResponseWriter, r *http.Request) {
	testId := mux.Vars(r)["testId"]
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   "trigger",
	})

	trigger, _ := json.Marshal(dlts.Trigger{
		TestId:  testId,
		Trigger: "YES",
	})

	writer.WriteMessages(context.Background(), kafka.Message{
		Value: trigger,
	})

	writer.Close()
	w.WriteHeader(http.StatusOK)
}

func Home(w http.ResponseWriter, r *http.Request) {
	tmpl := template.Must(template.ParseFiles("templates/index.html"))

	w.WriteHeader(http.StatusOK)

	tmpl.Execute(w, nil)
}

func main() {
	r := mux.NewRouter()
	broker = os.Getenv("BROKER_IP")
	nodes = make(map[string]*struct {
		Up     bool
		Last   time.Time
		NodeIp string
	})

	registerStopChan := make(chan struct{})
	heartbeatStopChan := make(chan struct{})
	metricsStopChan := make(chan struct{})

	go func(stopChan chan struct{}) {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker},
			Topic:   "register",
		})

		for {
			select {
			case <-stopChan:
				r.Close()
			default:
				message, err := r.ReadMessage(context.Background())
				if err != nil {
					fmt.Println(err.Error())
					continue
				}

				var register dlts.Register
				fmt.Println("Recvd Register: ", string(message.Value))
				json.Unmarshal(message.Value, &register)

				nodesLock.Lock()
				nodes[register.NodeId] = &struct {
					Up     bool
					Last   time.Time
					NodeIp string
				}{}
				nodes[register.NodeId].Up = true
				nodes[register.NodeId].Last = time.Now()
				nodes[register.NodeId].NodeIp = register.NodeIp
				nodesLock.Unlock()
			}
		}
	}(registerStopChan)

	go func(stopChan chan struct{}) {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker},
			Topic:   "heartbeat",
		})

		for {
			select {
			case <-stopChan:
				r.Close()
			default:
				message, err := r.ReadMessage(context.Background())
				if err != nil {
					fmt.Println(err.Error())
				}

				var heartbeat dlts.Heartbeat
				fmt.Println("Recvd Heartbeat: ", string(message.Value))
				json.Unmarshal(message.Value, &heartbeat)

				nodesLock.Lock()
				if nodes[heartbeat.NodeId] == nil {
					nodes[heartbeat.NodeId] = &struct {
						Up     bool
						Last   time.Time
						NodeIp string
					}{}
				}
				nodes[heartbeat.NodeId].Last = time.Now()
				nodes[heartbeat.NodeId].Up = true
				nodesLock.Unlock()
			}
		}
	}(heartbeatStopChan)

	go func(stopChan chan struct{}) {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker},
			Topic:   "metrics",
		})

		for {
			select {
			case <-stopChan:
				r.Close()
			default:
				message, err := r.ReadMessage(context.Background())
				if err != nil {
					fmt.Println(err.Error())
				}

				var metrics dlts.Metrics
				fmt.Println("Recvd Metrics: ", string(message.Value))
				json.Unmarshal(message.Value, &metrics)

			}
		}
	}(metricsStopChan)

	r.HandleFunc("/", Home).Methods(http.MethodGet)
	r.HandleFunc("/avalanche", Avalanche).Methods(http.MethodPost)
	r.HandleFunc("/tsunami/{num}", Tsunami).Methods(http.MethodPost)
	r.HandleFunc("/metrics/{testId}", Metrics).Methods(http.MethodGet)
	r.HandleFunc("/trigger/{testId}", Trigger).Methods(http.MethodPost)
	r.HandleFunc("/tests", Tests).Methods(http.MethodGet)
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	http.ListenAndServe(":8001", r)
}
