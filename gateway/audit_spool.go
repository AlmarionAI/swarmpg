package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Only bounded metadata is retained: a statement digest, 200-character sample,
// and 1,000-character error. Query parameters and full results are never spooled.
type auditRecord struct {
	ID, AgentID, OrgID string
	At                 time.Time
	SQLHash, SQLSample string
	Params, Rows       int
	Bytes, Affected    int64
	MS                 float64
	SQLState, Error    *string
}

type auditEntry struct {
	record auditRecord
	active bool
}

type auditSpool struct {
	mu       sync.Mutex
	dir      string
	capacity int
	entries  map[string]auditEntry
	lock     *os.File
}

func auditID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func boundedText(s string, n int) string {
	var b strings.Builder
	for _, r := range s {
		if n <= 0 {
			break
		}
		// PostgreSQL text cannot represent NUL. Range also normalizes invalid
		// UTF-8 to U+FFFD, so rejected SQL cannot poison audit delivery. The
		// separate statement digest still covers the original bytes.
		if r == 0 {
			r = '\uFFFD'
		}
		b.WriteRune(r)
		n--
	}
	return b.String()
}

func newAuditSpool(dir string, capacity int) (*auditSpool, error) {
	if capacity < 1 {
		return nil, errors.New("audit spool capacity must be positive")
	}
	s := &auditSpool{dir: dir, capacity: capacity, entries: make(map[string]auditEntry)}
	if dir == "" {
		return s, nil
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("audit spool is already in use: %w", err)
	}
	s.lock = f
	files, err := os.ReadDir(dir)
	if err != nil {
		s.close()
		return nil, err
	}
	for _, file := range files {
		// A process may have stopped before renaming a temporary receipt. It
		// never admitted SQL on that unsynced intent, and we hold the sole lock.
		if !file.IsDir() && strings.HasPrefix(file.Name(), ".receipt-") {
			if err := os.Remove(filepath.Join(dir, file.Name())); err != nil {
				s.close()
				return nil, err
			}
			continue
		}
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		var r auditRecord
		data, readErr := os.ReadFile(filepath.Join(dir, file.Name()))
		if readErr == nil {
			readErr = json.Unmarshal(data, &r)
		}
		if readErr != nil || len(r.ID) != 36 || file.Name() != r.ID+".json" || len(r.SQLHash) != 64 {
			s.close()
			return nil, fmt.Errorf("invalid audit receipt %s; preserve and repair the spool", file.Name())
		}
		// Intents left by an interrupted process keep audit_outcome_unknown;
		// never infer that a missing completion means the SQL rolled back.
		s.entries[r.ID] = auditEntry{record: r}
	}
	// A smaller configuration never discards receipts already on disk. New
	// admissions fail until recovery drains the backlog below the new limit.
	return s, nil
}

func (s *auditSpool) close() {
	if s.lock != nil {
		_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		_ = s.lock.Close()
		s.lock = nil
	}
}

func (s *auditSpool) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

func (s *auditSpool) reserve(r auditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) >= s.capacity {
		return errors.New("audit spool full")
	}
	if err := s.persist(r); err != nil {
		return err
	}
	s.entries[r.ID] = auditEntry{record: r, active: true}
	return nil
}

func (s *auditSpool) complete(r auditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[r.ID]; !ok {
		return errors.New("audit receipt was not reserved")
	}
	err := s.persist(r)
	s.entries[r.ID] = auditEntry{record: r}
	return err
}

func (s *auditSpool) batch(limit int) []auditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch := make([]auditRecord, 0, len(s.entries))
	for _, e := range s.entries {
		if !e.active {
			batch = append(batch, e.record)
		}
	}
	sort.Slice(batch, func(i, j int) bool {
		if batch[i].At.Equal(batch[j].At) {
			return batch[i].ID < batch[j].ID
		}
		return batch[i].At.Before(batch[j].At)
	})
	if len(batch) > limit {
		batch = batch[:limit]
	}
	return batch
}

func (s *auditSpool) ack(batch []auditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dir != "" {
		for _, r := range batch {
			if err := os.Remove(filepath.Join(s.dir, r.ID+".json")); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		if err := syncAuditDir(s.dir); err != nil {
			return err
		}
	}
	for _, r := range batch {
		delete(s.entries, r.ID)
	}
	return nil
}

// A synced temporary file plus rename prevents partial JSON after a crash.
// Syncing the directory makes the intent durable before SQL admission returns.
func (s *auditSpool) persist(r auditRecord) error {
	if s.dir == "" {
		return nil
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.dir, ".receipt-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(s.dir, r.ID+".json")); err != nil {
		return err
	}
	return syncAuditDir(s.dir)
}

func syncAuditDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
