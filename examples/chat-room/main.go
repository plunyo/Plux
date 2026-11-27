package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	plux "github.com/plunyo/Plux"
)

type ChatMessage struct {
	Name string `json:"name"`
	Text string `json:"text"`
}

func main() {
	var isClient, isServer bool

	flag.BoolVar(&isClient, "c", false, "run in client mode")
	flag.BoolVar(&isClient, "client", false, "run in client mode")

	flag.BoolVar(&isServer, "s", false, "run in server mode")
	flag.BoolVar(&isServer, "server", false, "run in server mode")

	address := flag.String("addr", "127.0.0.1:9098", "server address")
	listen := flag.String("listen", ":9098", "server listen address")
	name := flag.String("name", "guest", "your username")

	flag.Parse()

	if isClient == isServer {
		fmt.Fprintln(os.Stderr, "Error: specify either --client or --server")
		flag.Usage()
		os.Exit(2)
	}

	var err error

	if isClient {
		err = handleClient(*address, *name)
	} else {
		err = handleServer(*listen)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func readInput() <-chan string {
	lines := make(chan string)

	go func() {
		defer close(lines)

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 1024), 1024 * 1024)

		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()

	return lines
}

func encodeMessage(message ChatMessage) ([]byte, error) {
	return json.Marshal(message)
}

func decodeMessage(data []byte) (ChatMessage, error) {
	var message ChatMessage

	err := json.Unmarshal(data, &message)
	return message, err
}

// --------------------------------------------------
// SERVER
// --------------------------------------------------

func handleServer(address string) error {
	node, err := plux.NewNode(address)
	if err != nil {
		return err
	}
	defer node.Close()

	if err := node.Start(); err != nil {
		return err
	}

	fmt.Println("=== PLUX CHAT SERVER ===")
	fmt.Println("listening on:", node.Addr())
	fmt.Println("type messages to broadcast")
	fmt.Println("type /quit to exit")
	fmt.Println()

	clients := make(map[*plux.Connection]struct{})

	events := node.Events()
	input := readInput()

	for {
		select {

		case event, ok := <-events:
			if !ok {
				return nil
			}

			switch event.Type {

			case plux.PeerConnected:
				conn := event.Connection
				clients[conn] = struct{}{}

				text := fmt.Sprintf("[%v] joined the chat", conn.ID)

				fmt.Println("[system]", text)

				sendToAll(clients, ChatMessage{
					Name: "system",
					Text: text,
				})

			case plux.PeerDisconnected:
				conn := event.Connection

				delete(clients, conn)

				text := fmt.Sprintf("[%v] left the chat", conn.ID)

				fmt.Println("[system]", text)

				sendToAll(clients, ChatMessage{
					Name: "system",
					Text: text,
				})

			case plux.MessageReceived:
				message, err := decodeMessage(event.Data)
				if err != nil {
					continue
				}

				fmt.Printf("[%s] %s\n", message.Name, message.Text)

				// Relay to every connected client.
				sendToAll(clients, message)

			case plux.ErrorOccurred:
				log.Println("network error:", string(event.Data))
			}

		case line, ok := <-input:
			if !ok {
				return nil
			}

			if line == "/quit" {
				return nil
			}

			if line == "" {
				continue
			}

			message := ChatMessage{
				Name: "server",
				Text: line,
			}

			fmt.Printf("[server] %s\n", line)

			sendToAll(clients, message)
		}
	}
}

func sendToAll(
	clients map[*plux.Connection]struct{},
	message ChatMessage,
) {
	data, err := encodeMessage(message)
	if err != nil {
		log.Println("encode error:", err)
		return
	}

	for conn := range clients {
		if !conn.Connected() {
			continue
		}

		if err := conn.Send(data); err != nil {
			log.Printf("send error to %v: %v", conn.ID, err)
		}
	}
}

// --------------------------------------------------
// CLIENT
// --------------------------------------------------

func handleClient(address, name string) error {
	node, err := plux.NewNode(":0")
	if err != nil {
		return err
	}
	defer node.Close()

	if err := node.Start(); err != nil {
		return err
	}

	conn, err := node.Connect(address)
	if err != nil {
		return err
	}

	fmt.Println("=== PLUX CHAT ===")
	fmt.Println("Connecting To:", address)
	fmt.Println("Username:", name)
	fmt.Println("Type /quit to exit")
	fmt.Println()

	connected := false

	events := node.Events()
	input := readInput()

	for {
		select {

		case event, ok := <-events:
			if !ok {
				return nil
			}

			switch event.Type {

			case plux.PeerConnected:
				connected = true

				fmt.Println("*** Connected to Chat Server ***")

			case plux.PeerDisconnected:
				connected = false

				fmt.Println("*** Disconnected from Chat Server ***")

				return nil

			case plux.MessageReceived:
				message, err := decodeMessage(event.Data)
				if err != nil {
					continue
				}

				fmt.Printf("[%s] %s\n", message.Name, message.Text)

			case plux.ErrorOccurred:
				log.Println("Network Error:", string(event.Data))
			}

		case line, ok := <-input:
			if !ok {
				return nil
			}

			if line == "/quit" {
				return nil
			}

			if line == "" {
				continue
			}

			if !connected || !conn.Connected() {
				fmt.Println("*** Not Connected ***")
				continue
			}

			message := ChatMessage{
				Name: name,
				Text: line,
			}

			data, err := encodeMessage(message)
			if err != nil {
				log.Println("Encode Error:", err)
				continue
			}

			if err := conn.Send(data); err != nil {
				log.Println("Send Error:", err)
			}
		}
	}
}
