package consumer

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	gde "github.com/joho/godotenv"
	"github.com/segfaultscribe/conduit/internal/checkpoint"
	"github.com/segfaultscribe/conduit/internal/event"
)

type Consumer struct {
	connStr      string
	checkpointer *checkpoint.Checkpointer
	relations    map[uint32]*pglogrepl.RelationMessage
	eventHandler func(ctx context.Context, event *event.ChangeEvent) error
}

// constructor
func New(
	connStr string,
	cp *checkpoint.Checkpointer,
	handler func(ctx context.Context, event *event.ChangeEvent) error,
) *Consumer {
	return &Consumer{
		connStr:      connStr,
		checkpointer: cp,
		relations:    make(map[uint32]*pglogrepl.RelationMessage),
		eventHandler: handler,
	}
}

// start method reconnection loop

// func (c *Consumer) Start(ctx context.Context) error {
// 	for {
// 		err :=
// 	}
// }

func (c *Consumer) run(ctx context.Context) error {
	// CONNECT AND START REPLICATION
	err := gde.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	// get the database URL from the environment file
	dbURL := os.Getenv("DB_URL")

	conn, err := pgconn.Connect(ctx, dbURL)
	if err != nil {
		log.Fatal("failed to connect to the DATABASE: ", err)
	}

	defer conn.Close(ctx)

	pluginArgs := []string{
		"proto_version '1'",
		"publication_names 'conduit_pub'",
	}

	lsn, err := c.checkpointer.Read()

	if err != nil {
		return fmt.Errorf("cannot read LSN!")
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
		log.Fatal("failed to start replication:", err)
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
				},
			)
			if err != nil {
				log.Fatal("failed to send heartbeat:", err)
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
			log.Fatal("receive error:", err)
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
				log.Fatal("ParsePrimaryKeepaliveMessage failed:", err)
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
					log.Fatal("failed to send immediate heartbeat:", err)
				}

				nextHeartbeat = time.Now().Add(5 * time.Second)
			}
			continue

		case pglogrepl.XLogDataByteID:
			// an actual WAL Content
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Fatal("parse error:", err)
			}

			pendingLSN := xld.WALStart

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				log.Fatal("logical parse error:", err)
			}

			event := c.decodeToEvent(logicalMsg, pendingLSN)
			if event != nil {
				err := c.eventHandler(ctx, event)
				if err != nil {
					return err
				}
				c.checkpointer.Write(pendingLSN)
			}
		}
	}

}

func (c *Consumer) decodeToEvent(msg pglogrepl.Message, lsn pglogrepl.LSN) *event.ChangeEvent {
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
		return event.NewChangeEvent(
			event.OperationInsert,
			rel.Namespace,
			rel.RelationName,
			lsn,
			decodeRow(rel, mgt.Tuple),
		)
	case *pglogrepl.UpdateMessage:
		rel, ok := c.relations[mgt.RelationID]
		if !ok {
			// Log it, skip it, but DO NOT crash.
			log.Printf("Warning: Received UPDATE for unknown relation ID %d", mgt.RelationID)
			return nil
		}
		return event.NewChangeEvent(
			event.OperationUpdate,
			rel.Namespace,
			rel.RelationName,
			lsn,
			decodeRow(rel, mgt.NewTuple),
		)
	case *pglogrepl.DeleteMessage:
		rel, ok := c.relations[mgt.RelationID]
		if !ok {
			// Log it, skip it, but DO NOT crash.
			log.Printf("Warning: Received DELETE for unknown relation ID %d", mgt.RelationID)
			return nil
		}
		return event.NewChangeEvent(
			event.OperationDelete,
			rel.Namespace,
			rel.RelationName,
			lsn,
			decodeRow(rel, mgt.OldTuple),
		)
	}
	return nil
}

func decodeRow(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]string {
	result := make(map[string]string)
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
