package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/segfaultscribe/conduit/pkg/checkpoint"
	"github.com/segfaultscribe/conduit/pkg/event"
	"github.com/segfaultscribe/conduit/pkg/sink"
)

type Consumer struct {
	connStr      string
	checkpointer *checkpoint.Checkpointer
	relations    map[uint32]*pglogrepl.RelationMessage
	// eventHandler func(ctx context.Context, event *event.ChangeEvent) error
	sink sink.Sink
}

// constructor
// func New(
// 	connStr string,
// 	cp *checkpoint.Checkpointer,
// 	// handler func(ctx context.Context, event *event.ChangeEvent) error,
// 	s sink.Sink,
// ) *Consumer {
// 	return &Consumer{
// 		connStr:      connStr,
// 		checkpointer: cp,
// 		relations:    make(map[uint32]*pglogrepl.RelationMessage),
// 		// eventHandler: handler,
// 		sink: s,
// 	}
// }

func New(
	dbURL string,
	checkpointFilePath string,
	sink sink.Sink,
) *Consumer {
	// ctx := context.Background()

	cp := checkpoint.New(checkpointFilePath)

	return &Consumer{
		connStr:      dbURL,
		checkpointer: cp,
		relations:    make(map[uint32]*pglogrepl.RelationMessage),
		sink:         sink,
	}
}

// start method reconnection loop

func (c *Consumer) Start(ctx context.Context) error {
	for {
		err := c.run(ctx)
		// When run returns because the context
		// was cancelled, it returns 'context.Canceled' as the error.
		// 'ctx.Err()' returns non-nil the moment context id done
		if ctx.Err() != nil {
			return nil
		}

		if err != nil {
			log.Printf("consumer error: %v - reconecting in 5s", err)
			// conc pattern to make sure time.After doesn't mess up safe shutdown
			select {
			case <-time.After(5 * time.Second):
				// need to reset relations before trying again
				c.relations = make(map[uint32]*pglogrepl.RelationMessage)
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (c *Consumer) run(ctx context.Context) error {

	conn, err := pgconn.Connect(ctx, c.connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to the DATABASE: %w", err)
	}

	defer conn.Close(ctx)

	if err := c.sink.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to SINK: %w", err)
	}

	pluginArgs := []string{
		"proto_version '1'",
		"publication_names 'conduit_pub'",
	}

	lsn, err := c.checkpointer.Read()

	if err != nil {
		return fmt.Errorf("cannot read LSN: %w", err)
	}

	err = pglogrepl.StartReplication(
		ctx,
		conn,
		"conduit_slot",
		lsn,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}
	log.Println("Replication started. Waiting for changes...")

	// RECEIVE LOOP
	// we need to send a heartbeat to ensure postgres doesn't disconect us
	nextHeartbeat := time.Now().Add(5 * time.Second)

	// // The relation cache - Postgres sends column definitions separately
	// // from row changes. We store them here and look them up when a row
	// // change arrives.
	// relations := map[uint32]*pglogrepl.RelationMessage{}

	// main loop
	for {
		if time.Now().After(nextHeartbeat) {
			err = pglogrepl.SendStandbyStatusUpdate(
				ctx,
				conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: pglogrepl.LSN(lsn),
					WALFlushPosition: pglogrepl.LSN(lsn),
					WALApplyPosition: pglogrepl.LSN(lsn),
				},
			)
			if err != nil {
				return fmt.Errorf("failed to send heartbeat: %w", err)
			}
			nextHeartbeat = time.Now().Add(5 * time.Second)
		}
		// deadline to not block forever
		ctx2, cancel := context.WithDeadline(ctx, nextHeartbeat)
		rawMsg, err := conn.ReceiveMessage(ctx2)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				// no messages arrived before deadline
				continue
			}
			return fmt.Errorf("receive error: %w", err)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		// look at the first byte to see what kind of a message we've got
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
			}

			if pkm.ReplyRequested {
				err = pglogrepl.SendStandbyStatusUpdate(
					ctx,
					conn,
					pglogrepl.StandbyStatusUpdate{
						WALWritePosition: pglogrepl.LSN(lsn),
						WALFlushPosition: pglogrepl.LSN(lsn),
						WALApplyPosition: pglogrepl.LSN(lsn),
						ReplyRequested:   false,
					},
				)
				if err != nil {
					return fmt.Errorf("failed to send immediate heartbeat: %w", err)
				}

				nextHeartbeat = time.Now().Add(5 * time.Second)
			}
			continue

		case pglogrepl.XLogDataByteID:
			// an actual WAL Content
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse error: %w", err)
			}

			pendingLSN := xld.WALStart

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("logical parse error: %w", err)
			}

			event := c.decodeToEvent(logicalMsg, pendingLSN)
			if event != nil {
				// err := c.eventHandler(ctx, event)
				if err := c.sink.Publish(ctx, event); err != nil {
					return fmt.Errorf("sink publish failed: %w", err)
				}

				if err := c.checkpointer.Write(pendingLSN); err != nil {
					return fmt.Errorf("failed to write checkpoint: %w", err)
				}
				lsn = pendingLSN
			}
		}
	}
}

func (c *Consumer) decodeToEvent(
	msg pglogrepl.Message,
	lsn pglogrepl.LSN,
) *event.ChangeEvent {
	// add decoding stuff
	switch mgt := msg.(type) {
	case *pglogrepl.BeginMessage:
		return nil
	case *pglogrepl.CommitMessage:
		return nil
	case *pglogrepl.RelationMessage:
		// what to do?
		c.relations[mgt.RelationID] = mgt
		return nil
	case *pglogrepl.InsertMessage:
		rel, ok := c.relations[mgt.RelationID]
		if !ok {
			// Log it, skip it, but DO NOT crash.
			log.Printf("Warning: Received INSERT for unknown relation ID %d", mgt.RelationID)
			return nil
		}
		e := event.NewChangeEvent(
			event.OperationInsert,
			rel.Namespace,
			rel.RelationName,
			lsn,
		)
		e.After = decodeRow(rel, mgt.Tuple)
		return e
	case *pglogrepl.UpdateMessage:
		rel, ok := c.relations[mgt.RelationID]
		if !ok {
			// Log it, skip it, but DO NOT crash.
			log.Printf("Warning: Received UPDATE for unknown relation ID %d", mgt.RelationID)
			return nil
		}
		e := event.NewChangeEvent(
			event.OperationUpdate,
			rel.Namespace,
			rel.RelationName,
			lsn,
		)
		if mgt.OldTuple != nil {
			e.Before = decodeRow(rel, mgt.OldTuple)
		}
		e.After = decodeRow(rel, mgt.NewTuple)
		return e
	case *pglogrepl.DeleteMessage:
		rel, ok := c.relations[mgt.RelationID]
		if !ok {
			// Log it, skip it, but DO NOT crash.
			log.Printf("Warning: Received DELETE for unknown relation ID %d", mgt.RelationID)
			return nil
		}
		e := event.NewChangeEvent(
			event.OperationDelete,
			rel.Namespace,
			rel.RelationName,
			lsn,
		)
		if mgt.OldTuple != nil {
			e.Before = decodeRow(rel, mgt.OldTuple)
		}
		return e
	}
	return nil
}

func decodeRow(
	rel *pglogrepl.RelationMessage,
	tuple *pglogrepl.TupleData,
) map[string]any {
	result := make(map[string]any)
	if tuple == nil {
		return result
	}
	for i, col := range tuple.Columns {
		colName := rel.Columns[i].Name
		switch col.DataType {
		case 'n':
			// null
			result[colName] = "NULL"
		case 't':
			// text
			result[colName] = string(col.Data)
		default:
			result[colName] = "(binary)"
		}
	}
	return result
}
