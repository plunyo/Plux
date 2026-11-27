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
	ackPacket
)

func createPacket(kind packetType, sequenceNumber uint32, payload []byte) *packet {
	return &packet{
		Type:           kind,
		SequenceNumber: sequenceNumber,
		Payload:        append([]byte(nil), payload...),
	}
}

func (packet *packet) serialize() ([]byte, error) {
	if len(packet.Payload) > maxPacketPayload {
		return nil, errors.New("packet payload too large")
	}

	raw := make([]byte, packetHeaderSize+len(packet.Payload))
	raw[0] = byte(packet.Type)

	binary.BigEndian.PutUint32(raw[1:5], packet.SequenceNumber)
	binary.BigEndian.PutUint16(raw[5:7], uint16(len(packet.Payload)))

	copy(raw[packetHeaderSize:], packet.Payload)

	return raw, nil
}

func deserializePacket(raw []byte) (*packet, error) {
	if len(raw) < packetHeaderSize {
		return nil, errors.New("packet too short")
	}

	payloadSize := int(binary.BigEndian.Uint16(raw[5:7]))

	if len(raw) != packetHeaderSize+payloadSize {
		return nil, errors.New("invalid packet payload size")
	}

	receivedPacketType := packetType(raw[0])

	if receivedPacketType > killAckPacket {
		return nil, errors.New("unknown packet type")
	}

	return &packet{
		Type:           receivedPacketType,
		SequenceNumber: binary.BigEndian.Uint32(raw[1:5]),
		Payload:        append([]byte(nil), raw[packetHeaderSize:]...),
	}, nil
}
