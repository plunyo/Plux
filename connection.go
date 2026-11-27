package plux

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type pendingPacket struct {
	packet   *packet
	lastSent time.Time
	retries  int
}

type Connection struct {
	id      uuid.UUID
	node    *Node
	address *net.UDPAddr

	sendSequence uint32
	mutex        sync.Mutex

	receivedMutex sync.Mutex
	received      map[uint32]struct{}

	pendingMutex sync.Mutex
	pending      map[uint32]*pendingPacket

	connected atomic.Bool
	closing   atomic.Bool
}

func (connection *Connection) ID() uuid.UUID {
	return connection.id
}

func (connection *Connection) Connected() bool {
	return connection.connected.Load()
}

func (connection *Connection) Send(data []byte) error {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()

	if !connection.connected.Load() {
		return errors.New("connection isn't established")
	}

	if connection.closing.Load() {
		return errors.New("connection is closing")
	}

	sequenceNumber := connection.sendSequence
	packet := createPacket(userPacket, sequenceNumber, data)

	connection.pendingMutex.Lock()
	connection.pending[sequenceNumber] = &pendingPacket{
		packet:   packet,
		lastSent: time.Now(),
		retries:  0,
	}
	connection.pendingMutex.Unlock()

	if err := connection.node.sendPacket(connection, packet); err != nil {
		connection.pendingMutex.Lock()
		delete(connection.pending, sequenceNumber)
		connection.pendingMutex.Unlock()

		return err
	}

	connection.sendSequence++
	return nil
}

func (connection *Connection) checkPending() {
	connection.pendingMutex.Lock()
	defer connection.pendingMutex.Unlock()

	for sequenceNumber, pendingPacket := range connection.pending {
		if time.Since(pendingPacket.lastSent) < retransmissionTimeout {
			continue
		}

		if pendingPacket.retries >= maxRetries {
			delete(connection.pending, sequenceNumber)
			continue
		}

		pendingPacket.lastSent = time.Now()
		pendingPacket.retries++

		connection.node.sendPacket(connection, pendingPacket.packet)
	}
}

func (connection *Connection) markReceived(sequenceNumber uint32) bool {
	connection.receivedMutex.Lock()
	defer connection.receivedMutex.Unlock()

	if _, exists := connection.received[sequenceNumber]; exists {
		return false
	}

	connection.received[sequenceNumber] = struct{}{}
	return true
}

func (connection *Connection) Close() error {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()

	if !connection.connected.Load() {
		return errors.New("connection is not established")
	}

	if !connection.closing.CompareAndSwap(false, true) {
		return nil
	}

	packet := createPacket(killPacket, 0, connection.id[:])

	if err := connection.node.sendPacket(connection, packet); err != nil {
		connection.closing.Store(false)
		return err
	}

	return nil
}
