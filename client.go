package plux

import (
	"net"

	"github.com/google/uuid"
)

func Connect(network string) (*Connection, error) {
	addr, err := net.ResolveUDPAddr("udp", network)
	if err != nil {
    	return nil, err
	}
	
	udpConn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
    	return nil, err
	}

	conn := Connection{
		ID: uuid.Must(uuid.NewRandom()),
		Conn: udpConn,
	}

	packet, err := CreatePacket(ConnPacket, conn.sendSeq)
	if err != nil {
    	return nil, err
	}
	
	conn.sendSeq++

	rawPacket, err := packet.Serialize()
	if err != nil {
    	return nil, err
	}
	
	udpConn.WriteToUDP(rawPacket, addr)

	return &conn, nil
}
