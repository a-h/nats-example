package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var natsURLFlag = flag.String("nats-url", nats.DefaultURL, "The URL of the nats server to publish to.")

func main() {
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cfg := Config{
		Log:     log,
		NATSURL: *natsURLFlag,
	}
	err := run(context.Background(), cfg)
	if err != nil {
		log.Error("Execution failed", slog.Any("error", err))
		os.Exit(1)
	}
}

type Config struct {
	Log     *slog.Logger
	NATSURL string
}

func run(ctx context.Context, cfg Config) (err error) {
	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		return err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              "subject_stream",
		Description:       "A stream of published items.",
		Subjects:          []string{"*"}, // Subscribe to everything.
		Retention:         jetstream.LimitsPolicy,
		MaxConsumers:      -1,
		MaxMsgs:           -1,
		MaxBytes:          1024 * 1024 * 1024,   // 1GB.
		Discard:           jetstream.DiscardNew, // If we hit the stream limit, don't let any more into the stream.
		MaxAge:            time.Hour * 24,       // Keep items in the stream for up to 24 hours.
		MaxMsgsPerSubject: -1,
		MaxMsgSize:        1024 * 256, // 256KB message size limit.
		Replicas:          1,          // We only have one node anyway!
		Storage:           jetstream.FileStorage,
	})
	if err != nil {
		return err
	}
	c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          "example_consumer",
		Durable:       "example_consumer", // Providing a durable consumer name means that the consumer will pick up where it left off when it's restarted.
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Hour, // Give a consumer up to an hour to process the message before trying again.
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	})
	if err != nil {
		return err
	}
	cctx, err := c.Consume(func(msg jetstream.Msg) {
		fmt.Println(string(msg.Data()))
		if err := msg.Ack(); err != nil {
			cfg.Log.Error("failed to acknowledge message", slog.Any("error", err))
		}
	})
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		cctx.Stop()
	}
	fmt.Println("returning")
	return err
}
