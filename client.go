package plux

import (
	"net"

	"github.com/google/uuid"
)

func Connect(address string) (*Connection, error) {
	udpConn, err := net.DialUDP(address, nil, nil)
	if err != nil {
		return nil, err
	}

	conn := Connection{
		ID: uuid.Must(uuid.NewRandom()),
		Conn: udpConn,
	}

	return &conn, nil
}
