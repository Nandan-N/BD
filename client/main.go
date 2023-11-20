package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
    broker := os.Getenv("BROKER_IP")
	topic := "heartbeat"

	stopChan := make(chan struct{})

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	readerConfig := kafka.ReaderConfig{
        Brokers: []string{broker},
		Topic:   topic,
	}
    reader := kafka.NewReader(readerConfig)

	go func() {
		for {
			select {
			case <-stopChan:
				reader.Close()
			default:
				m, err := reader.ReadMessage(context.Background())
				if err != nil {
					reader.Close()
					log.Fatal(err.Error())
				}
                fmt.Print("Read:")
				fmt.Println(string(m.Value))
			}
		}
	}()

    <-signalChan
    close(stopChan)

    fmt.Println("Exited gracefully")
}
