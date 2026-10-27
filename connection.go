package plux

import (
	"net"

	"github.com/google/uuid"
)

type Connection struct {
	ID uuid.UUID
	Conn *net.UDPConn
}
