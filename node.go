package plux

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Node struct {
	peers     map[uuid.UUID]*Connection
	addresses map[string]*Connection
	udpConn   *net.UDPConn

	events chan Event
	done   chan struct{}

	mutex     sync.RWMutex
	closeOnce sync.Once
	started   atomic.Bool
}

func NewNode(address string) (*Node, error) {
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	udpConn, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		return nil, err
	}

	return &Node{
		peers:     make(map[uuid.UUID]*Connection),
		addresses: make(map[string]*Connection),
		udpConn:   udpConn,
		events:    make(chan Event, eventBufferSize),
		done:      make(chan struct{}),
	}, nil
}

func (node *Node) Events() <-chan Event {
	return node.events
}

func (node *Node) Addr() *net.UDPAddr {
	udpAddress, _ := node.udpConn.LocalAddr().(*net.UDPAddr)
	return udpAddress
}

func (node *Node) Peers() int {
	node.mutex.RLock()
	defer node.mutex.RUnlock()

	count := 0

	for _, connection := range node.peers {
		if connection.Connected() {
			count++
		}
	}

	return count
}

func (node *Node) Connect(address string) (*Connection, error) {
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	addressKey := udpAddress.String()

	node.mutex.Lock()

	if existing := node.addresses[addressKey]; existing != nil {
		node.mutex.Unlock()
		return nil, errors.New("already connected or connection pending")
	}

	connection := &Connection{
		id:            uuid.New(),
		node:          node,
		address:       udpAddress,
		received:      make(map[uint32]struct{}),
		pending:       make(map[uint32]*pendingPacket),
	}

	node.peers[connection.id] = connection
	node.addresses[addressKey] = connection

	node.mutex.Unlock()

	packet := createPacket(connPacket, 0, connection.id[:])

	if err := node.sendPacket(connection, packet); err != nil {
		node.removeConnection(connection)
		return nil, err
	}

	return connection, nil
}

func (node *Node) Start() error {
    go func() {
        if err := node.Serve(); err != nil {
            node.emit(Event{
                Type: ErrorOccurred,
                Data: []byte(err.Error()),
            })
        }
    }()

    return nil
}

func (node *Node) Serve() error {
	if !node.started.CompareAndSwap(false, true) {
		return errors.New("node is already serving")
	}

	defer close(node.events)

	go node.retransmissionLoop()

	buffer := make([]byte, 65535)

	for {
		size, udpAddress, err := node.udpConn.ReadFromUDP(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}

			return err
		}

		packet, err := deserializePacket(buffer[:size])
		if err != nil {
			continue
		}

		if err := node.handlePacket(udpAddress, packet); err != nil {
			return fmt.Errorf("handling packet: %w", err)
		}
	}
}

func (node *Node) Close() error {
	var err error

	node.closeOnce.Do(func() {
		close(node.done)
		err = node.udpConn.Close()
	})

	return err
}

func (node *Node) handlePacket(udpAddress *net.UDPAddr, packet *packet) error {
	addressKey := udpAddress.String()

	switch packet.Type {
	case connPacket:
		connectionID, err := uuid.FromBytes(packet.Payload)
		if err != nil {
			return nil
		}

		node.mutex.Lock()

		connection := node.addresses[addressKey]

		if connection == nil {
			connection = &Connection{
				id:       connectionID,
				node:     node,
				address:  udpAddress,
				received: make(map[uint32]struct{}),
				pending:  make(map[uint32]*pendingPacket),
			}

			node.peers[connectionID] = connection
			node.addresses[addressKey] = connection
		}

		node.mutex.Unlock()

		acknowledgement := createPacket(connAckPacket, 0, connectionID[:])

		if err := node.sendPacket(connection, acknowledgement); err != nil {
			return err
		}

		if connection.connected.CompareAndSwap(false, true) {
			node.emit(Event{
				Type:       PeerConnected,
				Connection: connection,
			})
		}

	case connAckPacket:
		connectionID, err := uuid.FromBytes(packet.Payload)
		if err != nil {
			return nil
		}

		node.mutex.RLock()
		connection := node.addresses[addressKey]
		node.mutex.RUnlock()

		if connection == nil || connection.id != connectionID {
			return nil
		}

		if connection.connected.CompareAndSwap(false, true) {
			node.emit(Event{
				Type:       PeerConnected,
				Connection: connection,
			})
		}

	case userPacket:
		node.mutex.RLock()
		connection := node.addresses[addressKey]
		node.mutex.RUnlock()

		if connection == nil || !connection.connected.Load() || connection.closing.Load() {
			return nil
		}

		acknowledgement := createPacket(ackPacket, packet.SequenceNumber, nil)

		if err := node.sendPacket(connection, acknowledgement); err != nil {
			return err
		}

		if !connection.markReceived(packet.SequenceNumber) {
			return nil
		}

		node.emit(Event{
			Type:       MessageReceived,
			Connection: connection,
			Data:       append([]byte(nil), packet.Payload...),
		})

	case ackPacket:
		node.mutex.RLock()
		connection := node.addresses[addressKey]
		node.mutex.RUnlock()

		if connection == nil {
			return nil
		}

		connection.pendingMutex.Lock()
		delete(connection.pending, packet.SequenceNumber)
		connection.pendingMutex.Unlock()

	case killPacket:
		connectionID, err := uuid.FromBytes(packet.Payload)
		if err != nil {
			return nil
		}

		node.mutex.RLock()
		connection := node.addresses[addressKey]
		node.mutex.RUnlock()

		if connection == nil || connection.id != connectionID {
			return nil
		}

		acknowledgement := createPacket(killAckPacket, 0, connectionID[:])

		if err := node.sendPacket(connection, acknowledgement); err != nil {
			return err
		}

		node.removeConnection(connection)
		connection.connected.Store(false)

		node.emit(Event{
			Type:       PeerDisconnected,
			Connection: connection,
		})

	case killAckPacket:
		connectionID, err := uuid.FromBytes(packet.Payload)
		if err != nil {
			return nil
		}

		node.mutex.RLock()
		connection := node.addresses[addressKey]
		node.mutex.RUnlock()

		if connection == nil || connection.id != connectionID {
			return nil
		}

		node.removeConnection(connection)
		connection.connected.Store(false)

		node.emit(Event{
			Type:       PeerDisconnected,
			Connection: connection,
		})
	}

	return nil
}

func (node *Node) sendPacket(connection *Connection, packet *packet) error {
	raw, err := packet.serialize()
	if err != nil {
		return err
	}

	_, err = node.udpConn.WriteToUDP(raw, connection.address)
	return err
}

func (node *Node) removeConnection(connection *Connection) {
	node.mutex.Lock()
	delete(node.peers, connection.id)
	delete(node.addresses, connection.address.String())
	node.mutex.Unlock()
}

func (node *Node) emit(event Event) {
	select {
	case node.events <- event:
	case <-node.done:
	}
}

func (node *Node) retransmissionLoop() {
	ticker := time.NewTicker(retransmissionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			node.mutex.RLock()

			connections := make([]*Connection, 0, len(node.peers))

			for _, connection := range node.peers {
				connections = append(connections, connection)
			}

			node.mutex.RUnlock()

			for _, connection := range connections {
				connection.checkPending()
			}

		case <-node.done:
			return
		}
	}
}
