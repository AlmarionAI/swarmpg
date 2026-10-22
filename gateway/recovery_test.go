package main

import (
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestRecoveryConfigurationRequiresCompletePins(t *testing.T) {
	for _, tc := range []struct {
		name, system, timeline, generation string
		valid                              bool
	}{
		{"disabled", "", "", "", true},
		{"enabled", "18446744073709551615", "2", "1", true},
		{"missing generation", "12", "2", "", false},
		{"missing system", "", "2", "1", false},
		{"missing timeline", "12", "", "1", false},
		{"zero generation", "12", "2", "0", false},
		{"overflow generation", "12", "2", "2147483648", false},
		{"overflow system", "18446744073709551616", "2", "1", false},
		{"overflow timeline", "12", "4294967296", "1", false},
		{"noncanonical", "012", "2", "1", false},
		{"noncanonical timeline", "12", "+2", "1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RECOVERY_SYSTEM_ID", tc.system)
			t.Setenv("RECOVERY_TIMELINE", tc.timeline)
			t.Setenv("RECOVERY_GENERATION", tc.generation)
			cfg, err := recoveryConfigFromEnv()
			if (err == nil) != tc.valid {
				t.Fatalf("config=%v err=%v", cfg, err)
			}
			if tc.name == "disabled" && cfg != nil {
				t.Fatal("unexpected implicit recovery activation")
			}
		})
	}
}

func TestRuntimeURLPreservesFailoverAndTLSOptions(t *testing.T) {
	u, err := runtimeDatabaseURL("postgres://admin:secret@primary:5432,standby:5433/db?sslmode=verify-full&sslrootcert=%2Ftmp%2Fca.pem&target_session_attrs=read-write&connect_timeout=2&password=alsosecret&user=admin")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(u, "secret") || strings.Contains(u, "admin") {
		t.Fatal("inherited admin credentials")
	}
	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.User.Username() != "agent_runtime" || parsed.Host != "primary:5432,standby:5433" || parsed.Query().Get("sslmode") != "verify-full" || parsed.Query().Get("sslrootcert") != "/tmp/ca.pem" || parsed.Query().Get("target_session_attrs") != "read-write" {
		t.Fatalf("lost failover or TLS configuration: %s", u)
	}
}

// No actual CA file is needed to prove URI options survive derivation; parsing
// with libpq verifies host selection separately below.
func TestRuntimeURLFallbackParsing(t *testing.T) {
	u, err := runtimeDatabaseURL("postgres://admin:secret@primary:5432,standby:5433/db?sslmode=disable&target_session_attrs=read-write&connect_timeout=2")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(u)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "agent_runtime" || len(cfg.Fallbacks) != 1 || cfg.Fallbacks[0].Host != "standby" || cfg.ValidateConnect == nil {
		t.Fatalf("lost failover options: %s", u)
	}
}

func TestSyncCommitCancellationNeverAcknowledged(t *testing.T) {
	for _, state := range []string{"01000", "57014", "57P01"} {
		t.Run(state, func(t *testing.T) {
			cfg := &recoveryConfig{}
			conn := &pgconn.PgConn{}
			cfg.committing.Store(conn, true)
			cfg.observeNotice(conn, &pgconn.Notice{Severity: "localized warning", SeverityUnlocalized: "WARNING", Code: state, Message: "localized server warning"})
			if _, ok := cfg.uncertain.Load(conn); !ok {
				t.Fatal("unreplicated commit warning ignored")
			}
			failure := classifyPg(commitUnconfirmed())
			if failure.Status != 503 || failure.Code != "commit_unconfirmed" || !failure.Retryable {
				t.Fatalf("unsafe commit outcome: %+v", failure)
			}
		})
	}
}

func TestOrdinaryNoticeDoesNotPoisonConnection(t *testing.T) {
	cfg := &recoveryConfig{}
	conn := &pgconn.PgConn{}
	cfg.observeNotice(conn, &pgconn.Notice{Severity: "WARNING", Code: "01000"})
	cfg.committing.Store(conn, true)
	cfg.observeNotice(conn, &pgconn.Notice{Severity: "NOTICE", Code: "00000"})
	if _, bad := cfg.uncertain.Load(conn); bad {
		t.Fatal("notice outside COMMIT or informational notice poisoned connection")
	}
}
