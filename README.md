# plux

A small UDP networking library built around nodes, connections and events.

## Start a node

```go
node, err := plux.NewNode(":8080")
if err != nil {
    log.Fatal(err)
}
defer node.Close()

go node.Serve()

for event := range node.Events() {
    switch event.Type {
    case plux.PeerConnected:
        fmt.Println("connected:", event.Connection.ID())
    case plux.PeerDisconnected:
        fmt.Println("disconnected:", event.Connection.ID())
    case plux.MessageReceived:
        fmt.Println(string(event.Data))
    }
}
```

## Connect to another node

Both nodes call `NewNode` and run `Serve`. One node then calls:

```go
connection, err := node.Connect("127.0.0.1:8080")
if err != nil {
    log.Fatal(err)
}
```

Wait for the `PeerConnected` event before sending:

```go
if err := connection.Send([]byte("hello")); err != nil {
    log.Fatal(err)
}
```

Close a connection with `connection.Close()` and close the node with `node.Close()`.

## Current scope

- One UDP socket per node.
- A node can accept and initiate connections.
- Connection, disconnection and message events.
- Packet serialization stays internal to the package.
- Sequence numbers are included in packets but reliability, retransmission and ordering are not implemented yet. UDP can drop or reorder datagrams, and the current handshake does not retry lost packets.
