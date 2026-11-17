package plux

import (
	"bytes"
	"encoding/binary"
)

type PacketType int

type Packet struct {
	Type PacketType
	Payload []byte

	SequenceNumber uint32
}

const (
	AckPacket PacketType = iota
	UserPacket

	ConnPacket
	ConnAckPacket
	
	SynPacket
	SynAckPacket
	
	KillPacket
	KillAckPacket
)

func CreatePacket(packetType PacketType, seq uint32) (*Packet, error) {
	return &Packet{
		Type: packetType,
		Payload: make([]byte, 0),
		SequenceNumber: seq,
	}, nil
}

func (p *Packet) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.LittleEndian, p.Type)
	if err != nil {
		return nil, err
	}

	err = binary.Write(&buf, binary.LittleEndian, p.Payload)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}


func DeserializePacket(rawPacket []byte) (*Packet, error) {
	var buf bytes.Buffer

	var packetType PacketType
	var payload []byte

	err := binary.Read(&buf, binary.LittleEndian, &packetType)
	if err != nil {
		return nil, err
	}

	err = binary.Read(&buf, binary.LittleEndian, &payload)
	if err != nil {
		return nil, err
	}

	return &Packet{
		Type: packetType,
		Payload: payload,
	}, nil
}
