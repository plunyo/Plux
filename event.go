package plux

type EventType uint8

const (
	PeerConnected EventType = iota
	PeerDisconnected
	MessageReceived
)

type Event struct {
	Type       EventType
	Connection *Connection
	Data       []byte
}
