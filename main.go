package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	gde "github.com/joho/godotenv"
)

func main() {
	// load the dot env function
	err := gde.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	// get the database URL from the environment file
	dbURL := os.Getenv("DB_URL")

	ctx := context.Background()
	// create the connection with context

	conn, err := pgconn.Connect(ctx, dbURL)
	if err != nil {
		log.Fatal("failed to connect to the DATABASE: ", err)
	}

	defer conn.Close(ctx)

	pluginArgs := []string{
		"proto_version '1'",
		"publication_names 'conduit_pub'",
	}

	err = pglogrepl.StartReplication(
		ctx,
		conn,
		"conduit_slot",
		0,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs,
		},
	)
	if err != nil {
		log.Fatal("failed to start replication:", err)
	}
	log.Println("Replication started. Waiting for changes...")

	// we need to send a heartbeat to ensure postgres doesn't disconect us
	nextHeartbeat := time.Now().Add(5 * time.Second)

	// The relation cache - Postgres sends column definitions separately
	// from row changes. We store them here and look them up when a row
	// change arrives.
	relations := map[uint32]*pglogrepl.RelationMessage{}

	// main loop
	for {
		if time.Now().After(nextHeartbeat) {
			err = pglogrepl.SendStandbyStatusUpdate(
				ctx,
				conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: pglogrepl.LSN(0),
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
			continue

		case pglogrepl.XLogDataByteID:
			// an actual WAL Content
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Fatal("parse error:", err)
			}

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				log.Fatal("logical parse error:", err)
			}

			switch m := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				// relation message means the information about a
				// tables structure. Postgres sends this once when a
				// change in this table is encountered
				// we need to keep a map of this so that we know
				// the table schema for future messages
				relations[m.RelationID] = m
				log.Printf(
					"RELATION: table=%s column=%d\n",
					m.RelationName, len(m.Columns),
				)

			case *pglogrepl.InsertMessage:
				rel, ok := relations[m.RelationID]
				if !ok {
					log.Println("[INSERT] unknown realtionID, skipping...")
					continue
				}
				values := decodeRow(rel, m.Tuple)
				log.Printf(
					"INSERT: table=%s values=%v\n",
					rel.RelationName, values,
				)

			case *pglogrepl.UpdateMessage:
				rel, ok := relations[m.RelationID]
				if !ok {
					log.Println("[UPDATE] unknown relationID, skipping...")
					continue
				}
				values := decodeRow(rel, m.NewTuple)
				log.Printf(
					"UPDATE: table=%s new_values=%v\n",
					rel.RelationName, values,
				)

			case *pglogrepl.DeleteMessage:
				rel, ok := relations[m.RelationID]
				if !ok {
					log.Println("[DELETE] unknown relationID, skipping...")
					continue
				}
				log.Printf(
					"DELETE: table=%s relation_id=%d\n",
					rel.RelationName, m.RelationID,
				)

			case *pglogrepl.BeginMessage:
				log.Printf("BEGIN transaction xid=%d\n", m.Xid)

			case *pglogrepl.CommitMessage:
				log.Println("COMMIT")

			}
		}
	}
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
