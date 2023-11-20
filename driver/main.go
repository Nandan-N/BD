package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	dlts "github.com/achyuta116/big-data-projects/dlts/lib"
	"github.com/segmentio/kafka-go"
)

type Test struct {
	Config dlts.TestConfig
	Times  []float64
	Lock   sync.RWMutex
	Done   bool
}

var tests struct {
	Tests map[string]*Test
	Lock  sync.RWMutex
}

var (
    resource    = "http://server:8000"
	numRequests = 500
)

func getNodeId() string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, 10)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

func getNodeIp() string {
	hostname, _ := os.Hostname()
	addrs, _ := net.LookupHost(hostname)
	return addrs[0]
}

func register(broker string, nodeId string, nodeIp string) {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   "register",
	})

	register, _ := json.Marshal(dlts.Register{
		NodeId: nodeId,
		NodeIp: nodeIp,
	})

	w.WriteMessages(context.Background(), kafka.Message{
		Value: register,
	})

	w.Close()
}

func minimum(arr []float64) float64 {
	if len(arr) == 0 {
		return 0
	}

	min := arr[0]
	for _, value := range arr {
		if value < min {
			min = value
		}
	}
	return min
}

func maximum(arr []float64) float64 {
	if len(arr) == 0 {
		return 0
	}

	max := arr[0]
	for _, value := range arr {
		if value > max {
			max = value
		}
	}
	return max
}

func median(arr []float64) float64 {
	if len(arr) == 0 {
		return 0
	}

	sort.Float64s(arr)
	mid := len(arr) / 2
	if len(arr)%2 == 0 {
		return (arr[mid-1] + arr[mid]) / 2.0
	}
	return arr[mid]
}

func mean(arr []float64) float64 {
	if len(arr) == 0 {
		return 0
	}

	var sum float64
	for _, value := range arr {
		sum += value
	}
	return sum / float64(len(arr))
}

func calculateMetrics(times []float64) dlts.TestResult {
	return dlts.TestResult{
		Min:    minimum(times),
		Max:    maximum(times),
		Median: median(times),
		Mean:   mean(times),
	}
}

func avalancheTest(testId string) {
	var group sync.WaitGroup
	times := &tests.Tests[testId].Times
	lock := &tests.Tests[testId].Lock

	for i := 0; i < numRequests; i++ {
		group.Add(1)

		go func(group *sync.WaitGroup) {
			defer group.Done()

			start := time.Now()
			http.Get(resource)
			elapsed := time.Since(start)

			lock.Lock()
			*times = append(*times, float64(elapsed)/float64(time.Millisecond))
			lock.Unlock()
		}(&group)
	}

	group.Wait()
	lock.Lock()
	tests.Tests[testId].Done = true
	lock.Unlock()
}

func tsunamiTest(testId string) {
	times := &tests.Tests[testId].Times
	lock := &tests.Tests[testId].Lock

	for i := 0; i < 5; i++ {
		var group sync.WaitGroup

		for j := 0; j < numRequests/5; j++ {
			group.Add(1)
			go func(group *sync.WaitGroup) {
				defer group.Done()

				start := time.Now()
				http.Get(resource)
				elapsed := time.Since(start)

				lock.Lock()
				*times = append(*times, float64(elapsed)/float64(time.Millisecond))
				lock.Unlock()
			}(&group)
			group.Wait()
		}

		time.Sleep(time.Duration(tests.Tests[testId].Config.MessageDelay) * time.Second)
	}

	lock.Lock()
	tests.Tests[testId].Done = true
	lock.Unlock()
}

func main() {
	broker := os.Getenv("BROKER_IP")
	seed, _ := strconv.Atoi(os.Getenv("SEED"))
	rand.Seed(int64(seed))

	nodeId := getNodeId()
	nodeIp := getNodeIp()
	tests.Tests = make(map[string]*Test)

	testConfigStopChan := make(chan struct{})
	triggerStopChan := make(chan struct{})

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	register(broker, nodeId, nodeIp)

	go func() {
		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{broker},
			Topic:   "heartbeat",
		})

		for {
			message, _ := json.Marshal(dlts.Heartbeat{
				NodeId:    nodeId,
				Heartbeat: true,
			})
			w.WriteMessages(context.Background(), kafka.Message{
				Value: []byte(message),
			})

			time.Sleep(time.Second)
		}
	}()

	go func(stopChan chan struct{}) {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker},
			Topic:   "test_config",
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

				var config dlts.TestConfig
				fmt.Println("Recvd Test Config:", string(message.Value))
				json.Unmarshal(message.Value, &config)

				tests.Lock.Lock()
				tests.Tests[config.TestId] = &Test{
					Config: config,
					Times:  make([]float64, 0),
					Done:   false,
				}
				tests.Lock.Unlock()
			}
		}
	}(testConfigStopChan)

	go func(stopChan chan struct{}) {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker},
			Topic:   "trigger",
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

				var trigger dlts.Trigger
				fmt.Println("Recvd Trigger:", string(message.Value))
				json.Unmarshal(message.Value, &trigger)

				if tests.Tests[trigger.TestId].Config.Type == "AVALANCHE" {
					go avalancheTest(trigger.TestId)
				} else if tests.Tests[trigger.TestId].Config.Type == "TSUNAMI" {
					go tsunamiTest(trigger.TestId)
				}
			}
		}
	}(triggerStopChan)

	go func() {
		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{broker},
			Topic:   "metrics",
		})

		for {
			time.Sleep(time.Second)

			var messages []kafka.Message
			tests.Lock.RLock()
			for k, v := range tests.Tests {
				var message kafka.Message
				v.Lock.RLock()
				result := calculateMetrics(v.Times)
				v.Lock.RUnlock()
				message.Value, _ = json.Marshal(dlts.Metrics{
					TestMetrics: result,
					NodeId:      nodeId,
					TestId:      k,
				})
				messages = append(messages, message)
			}
			tests.Lock.RUnlock()

			tests.Lock.Lock()
			toDelete := make([]string, 0)
			for k, v := range tests.Tests {
				if v.Done {
					toDelete = append(toDelete, k)
				}
			}

			for _, key := range toDelete {
                delete(tests.Tests, key)
			}

			tests.Lock.Unlock()

			w.WriteMessages(context.Background(), messages...)
		}
	}()

	<-signalChan
	close(triggerStopChan)
	close(testConfigStopChan)

	fmt.Println("Exited gracefully")
}
