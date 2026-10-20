package main

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// The memory functions in schema.sql raise SW409/SW404; agents must see them
// as 409 version_conflict (with the current version) and 404 not_found,
// whether they came through REST, MCP or raw SQL in the query tool.
func TestMemorySQLStatesMapToAgentErrors(t *testing.T) {
	e := classifyPg(&pgconn.PgError{Code: "SW409", Message: `version conflict on memory key "k"`,
		Detail: "current_version=4 expected_version=2", Hint: "Read the key again"})
	if e.Status != 409 || e.Code != "version_conflict" || e.Retryable {
		t.Fatalf("SW409: %+v", e)
	}
	body := errorBody(e)
	if body["current_version"] != int64(4) || body["hint"] == nil {
		t.Fatalf("conflict body: %v", body)
	}

	// a key that is absent reports current_version 0
	e = classifyPg(&pgconn.PgError{Code: "SW409", Detail: "current_version=0 expected_version=3"})
	if errorBody(e)["current_version"] != int64(0) {
		t.Fatalf("absent key: %v", errorBody(e))
	}

	e = classifyPg(&pgconn.PgError{Code: "SW404", Message: `memory key "k" has no history`})
	if e.Status != 404 || e.Code != "not_found" {
		t.Fatalf("SW404: %+v", e)
	}
}
