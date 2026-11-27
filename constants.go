package plux

import "time"

const (
	maxRetries = 5

	maxPacketPayload = 65535
	packetHeaderSize = 7
	eventBufferSize  = 64
)

const (
	retransmissionInterval = 100 * time.Millisecond
	retransmissionTimeout  = 500 * time.Millisecond
)
