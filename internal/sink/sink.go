package sink

// the idea is to create a pluggable sink such that the data captured can be injested into the
// preferred destination. This is to ensure that conduit doens't requre a specific system at the
// destination end to receive the captured data and can work with any medums implementing the requried methods

// an example of connecting kafka to the sink to make use of the captured data will be made
import (
	"context"

	"github.com/segfaultscribe/conduit/internal/event"
)

type Sink interface {
	// Establish whatever connection the sink needs.
	// For Kafka this means connecting to the broker.
	// For a webhook sink this might be validating the URL.
	// If this fails, the whole process should fail fast.
	// Should not run a pipeline with no working destination.
	Connect(ctx context.Context) error

	// HOT PATH runs potentially thousands of times per minute.
	// If this returns an error, the consumer treats it as a signal to stop and reconnect
	// same as any other error in the pipeline.
	Publish(ctx context.Context, event *event.ChangeEvent) error

	// Flush any buffers, close connections cleanly.
	// For Kafka this means flushing any batched messages that haven't been sent yet.
	// graceful shutdown
	Close()
}
