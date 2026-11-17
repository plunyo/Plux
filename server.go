package plux

import (
	"net"

	"github.com/google/uuid"
)

func Listen(network string) (*Host, error) {
	address, err := net.ResolveUDPAddr("udp", network)
	if err != nil {
		return nil, err
	}
	
	udpConn, err := net.ListenUDP("udp", address)
	if err != nil {
		return nil, err
	}

	host := Host{
		peers: make(map[uuid.UUID]*Connection, 0),
		conn: &Connection{
			ID: uuid.Must(uuid.NewRandom()),
			Conn: udpConn,
		},
	}

	return &host, nil
}
