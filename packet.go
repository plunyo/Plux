package plux

import (
	"encoding/binary"
	"errors"
)

type packetType uint8

type packet struct {
	Type           packetType
	SequenceNumber uint32
	Payload        []byte
}

const (
	connPacket packetType = iota
	connAckPacket
	userPacket
	killPacket
	killAckPacket
)

const packetHeaderSize = 7

func createPacket(packetType packetType, seq uint32, payload []byte) *packet {
	return &packet{
		Type:           packetType,
		SequenceNumber: seq,
		Payload:        append([]byte(nil), payload...),
	}
}

func (p *packet) serialize() ([]byte, error) {
	if len(p.Payload) > 65535 {
		return nil, errors.New("packet payload too large")
	}

	raw := make([]byte, packetHeaderSize+len(p.Payload))
	raw[0] = byte(p.Type)
	binary.LittleEndian.PutUint32(raw[1:5], p.SequenceNumber)
	binary.LittleEndian.PutUint16(raw[5:7], uint16(len(p.Payload)))
	copy(raw[packetHeaderSize:], p.Payload)

	return raw, nil
}

func deserializePacket(raw []byte) (*packet, error) {
	if len(raw) < packetHeaderSize {
		return nil, errors.New("packet too short")
	}

	payloadSize := int(binary.LittleEndian.Uint16(raw[5:7]))
	if len(raw) != packetHeaderSize+payloadSize {
		return nil, errors.New("invalid packet payload size")
	}

	packetType := packetType(raw[0])
	if packetType > killAckPacket {
		return nil, errors.New("unknown packet type")
	}

	return &packet{
		Type:           packetType,
		SequenceNumber: binary.LittleEndian.Uint32(raw[1:5]),
		Payload:        append([]byte(nil), raw[packetHeaderSize:]...),
	}, nil
}
