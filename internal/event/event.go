package event

import (
	"time"

	"github.com/jackc/pglogrepl"
)

// operation type
// schema
// table
// before values
// after values
// the LSN
// timestamp

type ChangeEvent struct {
	Operation Operation
	Schema    string
	Table     string
	Before    map[string]any
	After     map[string]any
	LSN       pglogrepl.LSN
	Timestamp time.Time
}

type Operation string

const (
	OperationInsert Operation = "INSERT"
	OperationUpdate Operation = "UPDATE"
	OperationDelete Operation = "DELETE"
)

func NewChangeEvent(
	op Operation,
	schema,
	table string,
	lsn pglogrepl.LSN,
) *ChangeEvent {
	return &ChangeEvent{
		Operation: op,
		Schema:    schema,
		Table:     table,
		LSN:       lsn,
		Timestamp: time.Now().UTC(),
	}
}
