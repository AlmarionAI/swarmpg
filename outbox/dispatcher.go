package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxGatewayBody = 1 << 20
	maxTargetBody  = 64 << 10
	gatewayTries   = 6
)

type config struct {
	Gateway, OrgID, Destination, Endpoint, WorkerID, AdminKey, TargetAuthorization string
	LeaseMS, Concurrency                                                           int
	PollInterval, RequestTimeout                                                   time.Duration
}

type message struct {
	ID             string          `json:"id"`
	OrgID          string          `json:"org_id"`
	TaskID         string          `json:"task_id"`
	Key            string          `json:"key"`
	Destination    string          `json:"destination"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
	State          string          `json:"state"`
	WorkerID       string          `json:"worker_id"`
	Fence          string          `json:"fence"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
}

type receipt struct {
	Message  *message `json:"message"`
	Replayed bool     `json:"replayed"`
}

func (r *receipt) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields["message"] == nil || fields["replayed"] == nil || bytes.Equal(fields["replayed"], []byte("null")) {
		return errors.New("incomplete delivery receipt")
	}
	type plain receipt
	return json.Unmarshal(data, (*plain)(r))
}

type dispatcher struct {
	cfg             config
	gateway, target *http.Client
	logger          *log.Logger
}

// Errors deliberately omit URLs, request/response bodies and underlying HTTP
// errors: those can contain credentials or arbitrary receiver secrets.
type gatewayError struct {
	status    int
	code      string
	retryable bool
}

func (e *gatewayError) Error() string {
	return fmt.Sprintf("gateway request failed (HTTP %d, code %s)", e.status, e.code)
}

var destinationPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func validUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return err == nil
}

func randomUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", errors.New("cannot create dispatcher request identity")
	}
	b[6], b[8] = (b[6]&15)|64, (b[8]&63)|128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func validateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return errors.New("URL must have a host and no userinfo or fragment")
	}
	if u.Scheme == "https" {
		return nil
	}
	if u.Scheme == "http" && (strings.EqualFold(u.Hostname(), "localhost") || net.ParseIP(u.Hostname()).IsLoopback()) {
		return nil
	}
	return errors.New("URL must use HTTPS or loopback HTTP")
}

func newDispatcher(cfg config, logger *log.Logger) (*dispatcher, error) {
	if err := validateURL(cfg.Gateway); err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	if err := validateURL(cfg.Endpoint); err != nil {
		return nil, fmt.Errorf("endpoint: %w", err)
	}
	u, _ := url.Parse(cfg.Gateway)
	if u.RawQuery != "" {
		return nil, errors.New("gateway URL must not contain a query")
	}
	if !validUUID(cfg.OrgID) || !destinationPattern.MatchString(cfg.Destination) {
		return nil, errors.New("require a valid organization UUID and destination name")
	}
	if cfg.WorkerID == "" {
		var err error
		cfg.WorkerID, err = randomUUID()
		if err != nil {
			return nil, err
		}
	}
	if !validUUID(cfg.WorkerID) || cfg.LeaseMS < 100 || cfg.LeaseMS > 300000 || cfg.Concurrency < 1 || cfg.Concurrency > 128 || cfg.PollInterval <= 0 || cfg.RequestTimeout <= 0 {
		return nil, errors.New("invalid worker UUID, lease, concurrency or timeout settings")
	}
	cfg.OrgID, cfg.WorkerID = strings.ToLower(cfg.OrgID), strings.ToLower(cfg.WorkerID)
	if cfg.AdminKey == "" || strings.ContainsAny(cfg.AdminKey+cfg.TargetAuthorization, "\r\n") {
		return nil, errors.New("OUTBOX_ADMIN_KEY is required; credentials must not contain line breaks")
	}
	cfg.Gateway = strings.TrimRight(cfg.Gateway, "/")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns, transport.MaxIdleConnsPerHost = cfg.Concurrency*4, cfg.Concurrency*2
	client := func() *http.Client {
		return &http.Client{Transport: transport.Clone(), Timeout: cfg.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &dispatcher{cfg: cfg, gateway: client(), target: client(), logger: logger}, nil
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func retryableGateway(status int, code string) bool {
	return status == 408 || status == 425 || status == 429 || status >= 500 || code == "40001" || code == "40P01" || code == "55P03" || code == "57P01" || code == "57P02" || code == "57P03" || strings.HasPrefix(code, "08")
}

func ownershipLost(err error) bool {
	var failure *gatewayError
	return errors.As(err, &failure) && failure.status == 409 && (failure.code == "stale_claim" || failure.code == "lease_expired")
}

// Encode once before retrying. Unknown claim responses retain this exact body
// and request ID across retry batches in loop(), not just within this function.
func (d *dispatcher) gatewayCall(ctx context.Context, path string, body []byte, out any) error {
	for attempt := 0; attempt < gatewayTries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, "POST", d.cfg.Gateway+path, bytes.NewReader(body))
		if err != nil {
			return &gatewayError{code: "invalid_request"}
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Admin-Key", d.cfg.AdminKey)
		resp, err := d.gateway.Do(req)
		failure := &gatewayError{code: "transport", retryable: true}
		if err == nil {
			data, readErr := io.ReadAll(io.LimitReader(resp.Body, maxGatewayBody+1))
			resp.Body.Close()
			failure.status = resp.StatusCode
			failure.retryable = resp.StatusCode == http.StatusOK || retryableGateway(resp.StatusCode, "")
			if readErr != nil || len(data) > maxGatewayBody {
				failure.code = "invalid_response"
			} else if resp.StatusCode == http.StatusOK {
				if json.Unmarshal(data, out) == nil {
					return nil
				}
				failure.code = "invalid_response"
			} else {
				var envelope struct {
					Error struct{ Code string } `json:"error"`
				}
				_ = json.Unmarshal(data, &envelope)
				failure.code = envelope.Error.Code
				if !destinationPattern.MatchString(failure.code) {
					failure.code = "unknown"
				}
				failure.retryable = retryableGateway(resp.StatusCode, failure.code)
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !failure.retryable || attempt == gatewayTries-1 {
			return failure
		}
		if !wait(ctx, time.Duration(100<<min(attempt, 4))*time.Millisecond) {
			return ctx.Err()
		}
	}
	panic("unreachable gateway retry loop")
}

func (d *dispatcher) mutation(ctx context.Context, m message, operation string, extra map[string]any, out any) error {
	extra["org_id"], extra["worker_id"], extra["fence"] = d.cfg.OrgID, d.cfg.WorkerID, m.Fence
	body, err := json.Marshal(extra)
	if err != nil {
		return errors.New("cannot encode delivery operation")
	}
	return d.gatewayCall(ctx, "/v1/admin/outbox/"+m.ID+"/"+operation, body, out)
}

func (d *dispatcher) envelope(m message) ([]byte, error) {
	fence, err := strconv.ParseInt(m.Fence, 10, 64)
	if err != nil || fence < 1 || !validUUID(m.ID) || !validUUID(m.TaskID) || m.OrgID != d.cfg.OrgID || m.Destination != d.cfg.Destination || m.WorkerID != d.cfg.WorkerID || m.State != "delivering" || m.Attempts < 1 || m.MaxAttempts != 10 || m.Attempts > m.MaxAttempts || !destinationPattern.MatchString(m.Key) || m.IdempotencyKey != "swarmpg:"+m.OrgID+":"+m.ID {
		return nil, errors.New("claim returned inconsistent delivery identity or ownership")
	}
	// UseNumber retains integer precision; Marshal sorts object keys, making
	// equivalent payload formatting independent of gateway serialization.
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(m.Payload))
	decoder.UseNumber()
	if decoder.Decode(&payload) != nil {
		return nil, errors.New("claim returned invalid payload")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return nil, errors.New("claim returned extra payload data")
	}
	return json.Marshal(struct {
		ID             string `json:"id"`
		OrgID          string `json:"org_id"`
		TaskID         string `json:"task_id"`
		Key            string `json:"key"`
		Destination    string `json:"destination"`
		Payload        any    `json:"payload"`
		IdempotencyKey string `json:"idempotency_key"`
	}{m.ID, m.OrgID, m.TaskID, m.Key, m.Destination, payload, m.IdempotencyKey})
}

func (d *dispatcher) renew(ctx context.Context, m message) error {
	var renewed message
	if err := d.mutation(ctx, m, "renew", map[string]any{"lease_ms": d.cfg.LeaseMS}, &renewed); err != nil {
		return err
	}
	if renewed.ID != m.ID || renewed.OrgID != m.OrgID || renewed.WorkerID != m.WorkerID || renewed.Fence != m.Fence || renewed.State != "delivering" {
		return errors.New("renewal returned inconsistent delivery ownership")
	}
	return nil
}

func (d *dispatcher) heartbeat(parent context.Context, m message) (context.Context, func() error) {
	ctx, cancel := context.WithCancel(parent)
	stopping, done := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var failure error // synchronized by close(done)
	go func() {
		defer close(done)
		for {
			timer := time.NewTimer(time.Duration(d.cfg.LeaseMS) * time.Millisecond / 3)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-stopping:
				timer.Stop()
				return
			case <-timer.C:
			}
			if err := d.renew(ctx, m); err != nil {
				failure = err
				cancel()
				return
			}
		}
	}()
	return ctx, func() error {
		once.Do(func() { close(stopping) })
		<-done
		cancel()
		return failure
	}
}

type deliveryOutcome struct {
	status    int
	errorText string
	permanent bool
}

func retryDelay(attempt int) int {
	return min(60000, 1000<<min(max(attempt-1, 0), 6))
}

func (d *dispatcher) send(ctx context.Context, m message, body []byte) deliveryOutcome {
	req, err := http.NewRequestWithContext(ctx, "POST", d.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return deliveryOutcome{errorText: "invalid configured receiver", permanent: true}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", m.IdempotencyKey)
	req.Header.Set("X-SwarmPG-Delivery-Fence", m.Fence)
	if d.cfg.TargetAuthorization != "" {
		req.Header.Set("Authorization", d.cfg.TargetAuthorization)
	}
	resp, err := d.target.Do(req)
	if err != nil {
		return deliveryOutcome{errorText: "receiver transport failure or timeout"}
	}
	// Receiver bodies are untrusted and never persisted. Stop reading at the
	// bound; a large body does not invalidate a known successful status.
	_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, maxTargetBody+1))
	resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if readErr != nil {
			return deliveryOutcome{errorText: "receiver response interrupted"}
		}
		return deliveryOutcome{status: resp.StatusCode}
	}
	transient := resp.StatusCode == 408 || resp.StatusCode == 425 || resp.StatusCode == 429 || resp.StatusCode >= 500
	return deliveryOutcome{status: resp.StatusCode, errorText: fmt.Sprintf("receiver returned HTTP %d", resp.StatusCode), permanent: !transient}
}

func (d *dispatcher) deliver(ctx context.Context, m message) error {
	body, err := d.envelope(m)
	if err != nil {
		return err
	}
	// A replayed claim can already have expired. Validate/extend ownership
	// before sending anything outside the database.
	if err := d.renew(ctx, m); err != nil {
		return err
	}
	workCtx, stop := d.heartbeat(ctx, m)
	defer stop()
	outcome := d.send(workCtx, m, body)
	if err := stop(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if outcome.errorText == "" {
		var ack receipt
		if err := d.mutation(ctx, m, "ack", map[string]any{"response": map[string]int{"status": outcome.status}}, &ack); err != nil {
			return err
		}
		if ack.Message == nil || ack.Message.ID != m.ID || ack.Message.OrgID != m.OrgID || ack.Message.State != "delivered" {
			return errors.New("acknowledgment returned inconsistent delivery state")
		}
		d.logger.Printf("delivered message=%s attempt=%d status=%d", m.ID, m.Attempts, outcome.status)
		return nil
	}
	var rejected message
	if err := d.mutation(ctx, m, "nack", map[string]any{"error": outcome.errorText, "retry_ms": retryDelay(m.Attempts), "permanent": outcome.permanent}, &rejected); err != nil {
		return err
	}
	if rejected.ID != m.ID || rejected.OrgID != m.OrgID || (rejected.State != "pending" && rejected.State != "failed") {
		return errors.New("negative acknowledgment returned inconsistent delivery state")
	}
	d.logger.Printf("deferred message=%s attempt=%d state=%s", m.ID, m.Attempts, rejected.State)
	return nil
}

func (d *dispatcher) loop(ctx context.Context) error {
	var pending []byte
	for ctx.Err() == nil {
		if pending == nil {
			id, err := randomUUID()
			if err != nil {
				return err
			}
			pending, _ = json.Marshal(map[string]any{"org_id": d.cfg.OrgID, "destination": d.cfg.Destination, "worker_id": d.cfg.WorkerID, "request_id": id, "lease_ms": d.cfg.LeaseMS})
		}
		var claimed receipt
		if err := d.gatewayCall(ctx, "/v1/admin/outbox/claim", pending, &claimed); err != nil {
			var failure *gatewayError
			if errors.As(err, &failure) && failure.retryable {
				d.logger.Printf("claim response uncertain; retaining request identity: %v", err)
				if !wait(ctx, time.Second) {
					return nil
				}
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		pending = nil
		if claimed.Message == nil {
			if !wait(ctx, d.cfg.PollInterval) {
				return nil
			}
			continue
		}
		if err := d.deliver(ctx, *claimed.Message); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if ownershipLost(err) {
				d.logger.Printf("relinquished message=%s after lease loss", claimed.Message.ID)
				continue
			}
			var failure *gatewayError
			if errors.As(err, &failure) && failure.retryable {
				d.logger.Printf("delivery disposition uncertain message=%s; lease will recover: %v", claimed.Message.ID, err)
				continue
			}
			return err
		}
	}
	return nil
}

func (d *dispatcher) run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer d.gateway.CloseIdleConnections()
	defer d.target.CloseIdleConnections()
	errors := make(chan error, d.cfg.Concurrency)
	var workers sync.WaitGroup
	for i := 0; i < d.cfg.Concurrency; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); errors <- d.loop(ctx) }()
	}
	first := <-errors
	cancel()
	workers.Wait()
	return first
}
