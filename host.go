package plux

import (
	"errors"
	"net"

	"github.com/google/uuid"
)

type Host struct {
	peers map[uuid.UUID]*Connection
	conn  *net.UDPConn
}

func (h *Host) Serve() error {
	buf := make([]byte, 65535)

	for {
		bytesRead, address, err := h.conn.ReadFromUDP(buf)
		if err != nil {
			return err
		}

		packet, err := DeserializePacket(buf[:bytesRead])
		if err != nil {
			continue
		}

		if err := h.handlePacket(address, packet); err != nil {
			return err
		}
	}
}

func (h *Host) handlePacket(address *net.UDPAddr, packet *Packet) error {
	switch packet.Type {
	case AckPacket:
		return h.handleAck(address, packet)

	case ConnPacket:
		return h.handleConn(address, packet)

	case ConnAckPacket:
		return h.handleConnAck(address, packet)

	case SynPacket:
		return h.handleSyn(address, packet)

	case SynAckPacket:
		return h.handleSynAck(address, packet)

	case KillPacket:
		return h.handleKill(address, packet)

	case KillAckPacket:
		return h.handleKillAck(address, packet)

	default:
		return errors.New("unexpected packet type")
	}
}

func (h *Host) handleAck(address *net.UDPAddr, packet *Packet) error {
	
}

func (h *Host) handleConn(address *net.UDPAddr, packet *Packet) error {
	
}

func (h *Host) handleConnAck(address *net.UDPAddr, packet *Packet) error {
	
}

func (h *Host) handleSyn(address *net.UDPAddr, packet *Packet) error {
	
}

func (h *Host) handleSynAck(address *net.UDPAddr, packet *Packet) error {
	
}

func (h *Host) handleKill(address *net.UDPAddr, packet *Packet) error {
	
}

func (h *Host) handleKillAck(address *net.UDPAddr, packet *Packet) error {
	
}

