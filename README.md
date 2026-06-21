# Conduit

Conduit is a lightweight, contract-driven Change Data Capture (CDC) engine for PostgreSQL written in Go. It tails the PostgreSQL Write-Ahead Log (WAL) using logical replication, decodes binary change events into predictable Go structs, and handles LSN checkpoint management completely out of sight. 

The framework uses a pluggable **Sink Architecture**, allowing developers to stream decoupled database events to any preferred destination (e.g., Kafka, stdout, or custom caching layers) simply by implementing a three-method interface.

## Core Architecture

Conduit acts as a managed pipeline between your data source and data destination.

```text
+-------------------+      +-------------------+      +-------------------+
|    PostgreSQL     | ---> |    Conduit Core   | ---> |    Custom Sink    |
| (Logical Repl.    |      | (WAL Decoding,    |      | (Kafka, Redis,    |
|   & WAL Tailing)  |      |  LSN Checkpoint)  |      |   Stdout, etc.)   |
+-------------------+      +-------------------+      +-------------------+
```

## Installation

```bash
go get github.com/segfaultscribe/conduit
```

Conduit allows the user to decide how to export and use the data extracted thorught the capture by using a pluggable sink interface. To connect to the sink, the user must implement the sink interface and pass it to the consumer on creation. The sink can be implemented according to the user's whims.

the sink:

```Go
type Sink interface {
	Connect(ctx context.Context) error
	Publish(ctx context.Context, event *event.ChangeEvent) error
	Close() error
}
```

Implement the three methods `Connect`, `Publish`, `Close` as per the requirements of your project. 

```Go
// When implementing the Sink interface, add this line to your package as a compile-time safety check:
var _ sink.Sink = (*YourSink)(nil)
```

>NOTE:
>When providing the DB connection URL for capture, make sure `replication=database` is present so that the connection can be made in replication mode to tail the WAL in postgresql. As of now conduit does not ensure this automatically and the connection will fail. 
