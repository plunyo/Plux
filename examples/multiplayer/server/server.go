package main

import (
	"encoding/json"
	"log"
	"math"
	"sort"
	"time"

	plux "github.com/plunyo/Plux"
)

const (
	port       = ":9000"
	worldWidth = 960
	worldHeight = 640
	playerSpeed = 250.0
	playerSize  = 16.0

	tickRate      = time.Second / 60
	broadcastRate = time.Second / 20
)

type Input struct {
	Up    bool `json:"up"`
	Down  bool `json:"down"`
	Left  bool `json:"left"`
	Right bool `json:"right"`
}

type ClientMessage struct {
	Type  string `json:"type"`
	Input Input  `json:"input"`
}

type PlayerState struct {
	ID uint64  `json:"id"`
	X  float32 `json:"x"`
	Y  float32 `json:"y"`
}

type ServerMessage struct {
	Type    string        `json:"type"`
	SelfID  uint64        `json:"self_id"`
	Players []PlayerState `json:"players"`
}

type Client struct {
	conn  *plux.Connection
	state PlayerState
	input Input
}

func clamp(value, min, max float32) float32 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func updatePlayer(client *Client, dt float64) {
	var dx, dy float64

	if client.input.Left {
		dx--
	}
	if client.input.Right {
		dx++
	}
	if client.input.Up {
		dy--
	}
	if client.input.Down {
		dy++
	}

	// Normalize diagonal movement.
	length := math.Hypot(dx, dy)

	if length > 0 {
		dx /= length
		dy /= length
	}

	client.state.X += float32(dx * playerSpeed * dt)
	client.state.Y += float32(dy * playerSpeed * dt)

	client.state.X = clamp(
		client.state.X,
		playerSize,
		worldWidth-playerSize,
	)

	client.state.Y = clamp(
		client.state.Y,
		playerSize,
		worldHeight-playerSize,
	)
}

func main() {
	node, err := plux.NewNode(port)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()

	if err := node.Start(); err != nil {
		log.Fatal(err)
	}

	log.Printf("server listening on %s", node.Addr())

	clients := make(map[*plux.Connection]*Client)

	var nextID uint64 = 1

	movementTicker := time.NewTicker(tickRate)
	broadcastTicker := time.NewTicker(broadcastRate)

	defer movementTicker.Stop()
	defer broadcastTicker.Stop()

	lastUpdate := time.Now()

	for {
		select {

		case event, ok := <-node.Events():
			if !ok {
				return
			}

			switch event.Type {

			case plux.PeerConnected:
				conn := event.Connection

				client := &Client{
					conn: conn,
					state: PlayerState{
						ID: nextID,
						X: float32(100 + (nextID*73)%760),
						Y: float32(100 + (nextID*47)%440),
					},
				}

				clients[conn] = client

				log.Printf(
					"player %d connected (%d players)",
					nextID,
					len(clients),
				)

				nextID++

			case plux.MessageReceived:
				client := clients[event.Connection]
				if client == nil {
					continue
				}

				var message ClientMessage

				if err := json.Unmarshal(event.Data, &message); err != nil {
					continue
				}

				if message.Type == "input" {
					client.input = message.Input
				}

			case plux.PeerDisconnected:
				client := clients[event.Connection]

				if client != nil {
					log.Printf("player %d disconnected", client.state.ID)
					delete(clients, event.Connection)
				}

			case plux.ErrorOccurred:
				log.Printf("network error: %s", event.Data)
			}

		case <-movementTicker.C:
			now := time.Now()

			dt := now.Sub(lastUpdate).Seconds()
			lastUpdate = now

			// Prevent enormous movement after a delayed tick.
			if dt > 0.05 {
				dt = 0.05
			}

			for _, client := range clients {
				updatePlayer(client, dt)
			}

		case <-broadcastTicker.C:

			// Build one world snapshot.
			players := make([]PlayerState, 0, len(clients))

			for _, client := range clients {
				players = append(players, client.state)
			}

			sort.Slice(players, func(i, j int) bool {
				return players[i].ID < players[j].ID
			})

			// Send the world snapshot to EVERY connected client.
			for _, client := range clients {
				message := ServerMessage{
					Type:    "snapshot",
					SelfID:  client.state.ID,
					Players: players,
				}

				data, err := json.Marshal(message)
				if err != nil {
					log.Println("marshal error:", err)
					continue
				}

				if err := client.conn.Send(data); err != nil {
					log.Printf(
						"send to player %d: %v",
						client.state.ID,
						err,
					)
				}
			}

			log.Printf(
				"players: %d | peers: %d",
				len(clients),
				node.Peers(),
			)
		}
	}
}
