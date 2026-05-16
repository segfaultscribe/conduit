package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jackc/pglogrepl"
)

// var fp string = "checkpoint.txt"

// Checkpointer manages the persistence of a Postgres Log Sequence Number (LSN).
// It ensures that the LSN is stored reliably on disk, allowing a replication
// stream to resume from the last successfully processed position after a restart.
type Checkpointer struct {
	filePath string
}

func New(fp string) *Checkpointer {
	return &Checkpointer{
		filePath: fp,
	}
}

// Read retrieves the persisted LSN from the checkpoint file.
// If the file does not exist, it returns LSN(0) and a nil error, signaling
// a fresh start. If the file is present but cannot be read or is corrupted,
// it returns a wrapped error.
func (c *Checkpointer) Read() (pglogrepl.LSN, error) {
	// open file
	fp := c.filePath
	data, err := os.Open(fp)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Println("File does not exist. Moving with LSN(0)")
			return pglogrepl.LSN(0), nil
		}
		return 0, fmt.Errorf("failed to open checkpoint file: %w", err)
	}
	defer data.Close()

	var lsnValue uint64
	err = binary.Read(data, binary.BigEndian, &lsnValue)
	if err != nil {
		// file exists, but corrupted
		return 0, fmt.Errorf("failed to read LSN data: %w", err)
	}
	return pglogrepl.LSN(lsnValue), nil
}

// Write atomically saves the LSN to disk using a "create-then-rename" strategy.
// This prevents data corruption by writing to a temporary file first and
// then performing an atomic rename. This ensures that even in the event of
// a crash or power failure, the checkpoint file remains in a valid state.
func (c *Checkpointer) Write(lsn pglogrepl.LSN) error {
	f, err := os.CreateTemp(filepath.Dir(c.filePath), "checkpoint-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	err = binary.Write(f, binary.BigEndian, uint64(lsn))
	if err != nil {
		return fmt.Errorf("failed to write to temp file: %w", err)
	}

	// because you should not try to rename an open file
	f.Close()

	err = os.Rename(f.Name(), c.filePath)
	if err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}
	return nil
}
