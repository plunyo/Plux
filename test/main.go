package main

import (
	"fmt"
	"log"

	plux "github.com/plunyo/Plux"
)

func main() {
	nodeA, err := plux.NewNode(":8080")
	if err != nil {
		log.Fatal(err)
	}
	defer nodeA.Close()

	nodeB, err := plux.NewNode(":8081")
	if err != nil {
		log.Fatal(err)
	}
	defer nodeB.Close()

	go func() {
		if err := nodeA.Serve(); err != nil {
			log.Println("[A] serve:", err)
		}
	}()

	go func() {
		if err := nodeB.Serve(); err != nil {
			log.Println("[B] serve:", err)
		}
	}()

	go func() {
		for event := range nodeA.Events() {
			switch event.Type {
			case plux.PeerConnected:
				fmt.Println("[A] connected:", event.Connection.ID())
				event.Connection.Send([]byte("hello from A"))

			case plux.PeerDisconnected:
				fmt.Println("[A] disconnected:", event.Connection.ID())

			case plux.MessageReceived:
				fmt.Printf("[A] received: %s\n", event.Data)
			}
		}
	}()

	go func() {
		for event := range nodeB.Events() {
			switch event.Type {
			case plux.PeerConnected:
				fmt.Println("[B] connected:", event.Connection.ID())

			case plux.PeerDisconnected:
				fmt.Println("[B] disconnected:", event.Connection.ID())

			case plux.MessageReceived:
				fmt.Printf("[B] received: %s\n", event.Data)
				event.Connection.Send([]byte("hello from B"))
			}
		}
	}()

	_, err = nodeA.Connect("127.0.0.1:8081")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("enter to stop.")
	fmt.Scanln()
}
