package plux

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

type Connection struct {
	id   uuid.UUID
	node *Node

	address *net.UDPAddr

	sendSeq uint32
	mu      sync.Mutex

	connected atomic.Bool
	closing   atomic.Bool
}

func (c *Connection) ID() uuid.UUID {
	return c.id
}

func (c *Connection) Connected() bool {
	return c.connected.Load()
}

func (c *Connection) Send(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected.Load() {
		return errors.New("connection isnt established")
	}
	
	if c.closing.Load() {
		return errors.New("connections closing")
	}

	p := createPacket(userPacket, c.sendSeq, data)
	if err := c.node.sendPacket(c, p); err != nil {
		return err
	}

	c.sendSeq++
	return nil
}

func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected.Load() {
		return errors.New("connection is not established")
	}
	
	if !c.closing.CompareAndSwap(false, true) {
		return nil
	}

	p := createPacket(killPacket, 0, c.id[:])
	if err := c.node.sendPacket(c, p); err != nil {
		c.closing.Store(false)
		return err
	}

	return nil
}
