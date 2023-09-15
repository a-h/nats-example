package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

var natsURLFlag = flag.String("nats-url", nats.DefaultURL, "The URL of the nats server to publish to.")

func main() {
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cfg := Config{
		Log:     log,
		NATSURL: *natsURLFlag,
	}
	err := run(cfg)
	if err != nil {
		log.Error("Execution failed", slog.Any("error", err))
		os.Exit(1)
	}
}

type Config struct {
	Log     *slog.Logger
	NATSURL string
}

func run(cfg Config) (err error) {
	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		return err
	}
	var i int
	for {
		if err := nc.Publish("subject", []byte(fmt.Sprintf("Message %d", i+1))); err != nil {
			cfg.Log.Error("failed to publish", slog.Any("error", err))
			continue
		}
		i++
		time.Sleep(time.Second)
	}
}
