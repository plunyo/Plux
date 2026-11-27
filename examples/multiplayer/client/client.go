package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	rl "github.com/gen2brain/raylib-go/raylib"
	plux "github.com/plunyo/Plux"
)

const (
	screenWidth  = 960
	screenHeight = 640

	playerRadius = 16

	sendRate = 50 * time.Millisecond
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

func getInput() Input {
	return Input{
		Up:    rl.IsKeyDown(rl.KeyW),
		Down:  rl.IsKeyDown(rl.KeyS),
		Left:  rl.IsKeyDown(rl.KeyA),
		Right: rl.IsKeyDown(rl.KeyD),
	}
}

func playerColor(id uint64) rl.Color {
	colors := []rl.Color{
		{R: 230, G: 41, B: 55, A: 255},
		{R: 0, G: 121, B: 241, A: 255},
		{R: 0, G: 228, B: 48, A: 255},
		{R: 255, G: 161, B: 0, A: 255},
		{R: 200, G: 122, B: 255, A: 255},
		{R: 255, G: 109, B: 194, A: 255},
	}

	return colors[id%uint64(len(colors))]
}

func main() {
	serverAddress := flag.String(
		"addr",
		"127.0.0.1:9000",
		"server UDP address",
	)
	flag.Parse()

	node, err := plux.NewNode(":0")
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()

	if err := node.Start(); err != nil {
		log.Fatal(err)
	}

	conn, err := node.Connect(*serverAddress)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("connecting to", *serverAddress)

	connected := false

	var selfID uint64
	var players []PlayerState

	lastSend := time.Now()

	rl.InitWindow(screenWidth, screenHeight, "Plux Multiplayer")
	defer rl.CloseWindow()

	rl.SetTargetFPS(60)

	for !rl.WindowShouldClose() {

		// Drain all pending network events.
		drainEvents:
		for {
			select {
			case event, ok := <-node.Events():
				if !ok {
					connected = false
					break drainEvents
				}

				switch event.Type {

				case plux.PeerConnected:
					connected = true
					log.Println("connected!")

				case plux.PeerDisconnected:
					connected = false
					players = nil
					log.Println("disconnected")

				case plux.MessageReceived:
					var message ServerMessage

					if err := json.Unmarshal(event.Data, &message); err != nil {
						continue
					}

					if message.Type == "snapshot" {
						selfID = message.SelfID
						players = message.Players
					}

				case plux.ErrorOccurred:
					log.Println("network error:", string(event.Data))
				}

			default:
				break drainEvents
			}
		}

		// Send keyboard input to the server.
		if connected && conn.Connected() &&
			time.Since(lastSend) >= sendRate {

			message := ClientMessage{
				Type:  "input",
				Input: getInput(),
			}

			data, err := json.Marshal(message)
			if err == nil {
				if err := conn.Send(data); err != nil {
					log.Println("send error:", err)
				}
			}

			lastSend = time.Now()
		}

		// ---------------- RENDERING ----------------

		rl.BeginDrawing()
		rl.ClearBackground(rl.RayWhite)

		// World boundary.
		rl.DrawRectangleLines(
			0,
			0,
			screenWidth,
			screenHeight,
			rl.LightGray,
		)

		// Render everyone.
		for _, player := range players {
			color := playerColor(player.ID)

			x := int32(player.X)
			y := int32(player.Y)

			rl.DrawCircle(
				x,
				y,
				playerRadius,
				color,
			)

			// Identify each player.
			label := fmt.Sprintf("Player %d", player.ID)

			if player.ID == selfID {
				label += " (YOU)"
			}

			rl.DrawText(
				label,
				x-35,
				y-35,
				14,
				rl.DarkGray,
			)
		}

		// Connection status.
		status := "CONNECTING..."

		if connected {
			status = "CONNECTED"
		}

		rl.DrawText(status, 15, 15, 20, rl.DarkGray)

		rl.DrawText(
			fmt.Sprintf("Players: %d", len(players)),
			15,
			40,
			18,
			rl.DarkGray,
		)

		rl.DrawText(
			"WASD to move",
			15,
			screenHeight-35,
			18,
			rl.DarkGray,
		)

		rl.EndDrawing()
	}
}
