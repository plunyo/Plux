package plux

import (
	"net"

	"github.com/google/uuid"
)

type Connection struct {
	ID uuid.UUID
	Conn *net.UDPConn
	
	sendSeq uint32
	recvSeq uint32
}

// structure is just uuid
func (c *Connection) Serialize() ([]byte, error) {
	rawPacket, err := c.ID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	
	return rawPacket, nil
}

func DeserializeConnection(raw []byte) (*Connection, error) {
	id, err := uuid.FromBytes(raw)
	if err != nil {
		return nil, err
	}

	return &Connection{ ID: id }, nil
}

func (c *Connection) SendPacket(packet Packet) error {
	rawPacket, err := packet.Serialize()
	if err != nil {
		return err
	}

	_, err = c.Conn.Write(rawPacket)
	if err != nil {
		return err
	}

	return nil
}

func (c *Connection) ReceivePacket() (*Packet, error) {
    buf := make([]byte, 65535)

    n, err := c.Conn.Read(buf)
    if err != nil {
        return nil, err
    }

    packet, err := DeserializePacket(buf[:n])
    if err != nil {
        return nil, err
    }

    return packet, nil
}
