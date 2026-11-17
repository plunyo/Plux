package plux

import (
	"bytes"
	"testing"
	"time"
)

func nextEvent(t *testing.T, events <-chan Event, eventType EventType) Event {
	t.Helper()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("event channel closed")
			}
			if event.Type == eventType {
				return event
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for event %v", eventType)
		}
	}
}

func TestNodeConnectionAndMessage(t *testing.T) {
	a, err := NewNode("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	b, err := NewNode("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	go a.Serve()
	go b.Serve()

	connection, err := a.Connect(b.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	nextEvent(t, b.Events(), PeerConnected)
	nextEvent(t, a.Events(), PeerConnected)

	want := []byte("hello from plux")
	if err := connection.Send(want); err != nil {
		t.Fatal(err)
	}

	event := nextEvent(t, b.Events(), MessageReceived)
	if !bytes.Equal(event.Data, want) {
		t.Fatalf("got %q, want %q", event.Data, want)
	}

	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}

	nextEvent(t, b.Events(), PeerDisconnected)
	nextEvent(t, a.Events(), PeerDisconnected)
}
