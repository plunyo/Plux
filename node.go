package plux

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

type Node struct {
	peers     map[uuid.UUID]*Connection
	addresses map[string]*Connection
	conn      *net.UDPConn

	events chan Event
	done   chan struct{}

	mu        sync.RWMutex
	closeOnce sync.Once
	started   atomic.Bool
}

func NewNode(address string) (*Node, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	return &Node{
		peers:     make(map[uuid.UUID]*Connection),
		addresses: make(map[string]*Connection),
		conn:      conn,
		events:    make(chan Event, 64),
		done:      make(chan struct{}),
	}, nil
}

func (n *Node) Events() <-chan Event {
	return n.events
}

func (n *Node) Addr() *net.UDPAddr {
	addr, _ := n.conn.LocalAddr().(*net.UDPAddr)
	return addr
}

func (n *Node) Peers() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	count := 0
	for _, connection := range n.peers {
		if connection.Connected() {
			count++
		}
	}

	return count
}

func (n *Node) Connect(address string) (*Connection, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	key := addr.String()

	n.mu.Lock()
	if existing := n.addresses[key]; existing != nil {
		n.mu.Unlock()
		return nil, errors.New("already connected or connection pending")
	}

	connection := &Connection{
		id:      uuid.New(),
		node:    n,
		address: addr,
	}
	n.peers[connection.id] = connection
	n.addresses[key] = connection
	n.mu.Unlock()

	p := createPacket(connPacket, 0, connection.id[:])
	if err := n.sendPacket(connection, p); err != nil {
		n.removeConnection(connection)
		return nil, err
	}

	return connection, nil
}

func (n *Node) Serve() error {
	if !n.started.CompareAndSwap(false, true) {
		return errors.New("node is already serving")
	}
	defer close(n.events)

	buf := make([]byte, 65535)

	for {
		size, address, err := n.conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		p, err := deserializePacket(buf[:size])
		if err != nil {
			continue
		}

		if err := n.handlePacket(address, p); err != nil {
			return fmt.Errorf("handling packet: %w", err)
		}
	}
}

func (n *Node) Close() error {
	var err error
	n.closeOnce.Do(func() {
		close(n.done)
		err = n.conn.Close()
	})
	return err
}

func (n *Node) handlePacket(address *net.UDPAddr, p *packet) error {
	key := address.String()

	switch p.Type {
	case connPacket:
		id, err := uuid.FromBytes(p.Payload)
		if err != nil {
			return nil
		}

		n.mu.Lock()
		connection := n.addresses[key]
		if connection == nil {
			connection = &Connection{
				id:      id,
				node:    n,
				address: address,
			}
			n.peers[id] = connection
			n.addresses[key] = connection
		}
		n.mu.Unlock()

		ack := createPacket(connAckPacket, 0, id[:])
		if err := n.sendPacket(connection, ack); err != nil {
			return err
		}

		if connection.connected.CompareAndSwap(false, true) {
			n.emit(Event{Type: PeerConnected, Connection: connection})
		}

	case connAckPacket:
		id, err := uuid.FromBytes(p.Payload)
		if err != nil {
			return nil
		}

		n.mu.RLock()
		connection := n.addresses[key]
		n.mu.RUnlock()
		if connection == nil || connection.id != id {
			return nil
		}

		if connection.connected.CompareAndSwap(false, true) {
			n.emit(Event{Type: PeerConnected, Connection: connection})
		}

	case userPacket:
		n.mu.RLock()
		connection := n.addresses[key]
		n.mu.RUnlock()
		if connection == nil || !connection.connected.Load() || connection.closing.Load() {
			return nil
		}

		n.emit(Event{
			Type:       MessageReceived,
			Connection: connection,
			Data:       append([]byte(nil), p.Payload...),
		})

	case killPacket:
		id, err := uuid.FromBytes(p.Payload)
		if err != nil {
			return nil
		}

		n.mu.RLock()
		connection := n.addresses[key]
		n.mu.RUnlock()
		if connection == nil || connection.id != id {
			return nil
		}

		ack := createPacket(killAckPacket, 0, id[:])
		if err := n.sendPacket(connection, ack); err != nil {
			return err
		}

		n.removeConnection(connection)
		connection.connected.Store(false)
		n.emit(Event{Type: PeerDisconnected, Connection: connection})

	case killAckPacket:
		id, err := uuid.FromBytes(p.Payload)
		if err != nil {
			return nil
		}

		n.mu.RLock()
		connection := n.addresses[key]
		n.mu.RUnlock()
		if connection == nil || connection.id != id {
			return nil
		}

		n.removeConnection(connection)
		connection.connected.Store(false)
		n.emit(Event{Type: PeerDisconnected, Connection: connection})
	}

	return nil
}

func (n *Node) sendPacket(connection *Connection, p *packet) error {
	raw, err := p.serialize()
	if err != nil {
		return err
	}

	_, err = n.conn.WriteToUDP(raw, connection.address)
	return err
}

func (n *Node) removeConnection(connection *Connection) {
	n.mu.Lock()
	delete(n.peers, connection.id)
	delete(n.addresses, connection.address.String())
	n.mu.Unlock()
}

func (n *Node) emit(event Event) {
	select {
	case n.events <- event:
	case <-n.done:
	}
}
