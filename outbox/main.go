// Outbox delivers committed intents to one operator-configured destination.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var cfg config
	flag.StringVar(&cfg.Gateway, "gateway", "http://127.0.0.1:8080", "SwarmPG gateway URL")
	flag.StringVar(&cfg.OrgID, "org-id", "", "organization UUID")
	flag.StringVar(&cfg.Destination, "destination", "", "configured outbox destination name")
	flag.StringVar(&cfg.Endpoint, "endpoint", "", "operator-configured receiver URL (HTTPS or loopback HTTP)")
	flag.StringVar(&cfg.WorkerID, "worker-id", "", "dispatcher UUID (default: random)")
	flag.IntVar(&cfg.LeaseMS, "lease-ms", 30000, "delivery lease in milliseconds (100–300000)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 4, "concurrent delivery loops (1–128)")
	flag.DurationVar(&cfg.PollInterval, "poll-interval", 250*time.Millisecond, "empty-queue polling interval")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", 10*time.Second, "timeout per gateway/receiver HTTP request")
	flag.Parse()
	cfg.AdminKey, cfg.TargetAuthorization = os.Getenv("OUTBOX_ADMIN_KEY"), os.Getenv("OUTBOX_TARGET_AUTHORIZATION")
	d, err := newDispatcher(cfg, log.New(os.Stderr, "outbox: ", log.LstdFlags))
	if err != nil {
		fmt.Fprintln(os.Stderr, "outbox:", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := d.run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "outbox:", err)
		os.Exit(1)
	}
}
