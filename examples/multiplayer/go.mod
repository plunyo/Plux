module plux-multiplayer-example

go 1.26.5

require (
	github.com/gen2brain/raylib-go/raylib v0.63.0
	github.com/plunyo/Plux v0.0.0
)

require (
	github.com/ebitengine/purego v0.10.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jupiterrider/ffi v0.8.0 // indirect
)

replace plux => ..

replace github.com/plunyo/Plux => ../..
