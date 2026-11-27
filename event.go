package plux

type EventType uint8

const (
	PeerConnected EventType = iota
	PeerDisconnected
	MessageReceived
	ErrorOccurred
)

type Event struct {
	Type       EventType
	Connection *Connection
	Data       []byte
}
