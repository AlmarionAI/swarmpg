package main

import (
	"encoding/json"
	"testing"
)

func TestFirstKeywordStripsCommentsAndWhitespace(t *testing.T) {
	cases := map[string]string{
		"SELECT 1":                             "SELECT",
		"  select 1":                           "SELECT",
		"\t\nSELECT 1":                         "SELECT",
		"/* c */ SELECT 1":                     "SELECT",
		"/* /* nested */ */ SET x=1":           "SET", // nested block comment must not hide SET
		"-- line\nSET x=1":                     "SET",
		"SET\tapp.agent_id='x'":                "SET", // tab after keyword
		"SET\napp.agent_id='x'":                "SET", // newline after keyword
		"(SELECT 1)":                           "SELECT",
		"  \t ":                                "",
		"/* only comment */":                   "",
		"WITH t AS (SELECT 1) SELECT * FROM t": "WITH",
		"creAte table x(i int)":                "CREATE",
		"'SELECT'||x":                          "", // starts with a string literal, not a keyword
	}
	for in, want := range cases {
		if got := firstKeyword(in); got != want {
			t.Errorf("firstKeyword(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestGuardRejectsForbiddenEvenObfuscated(t *testing.T) {
	a := &Agent{}
	bad := []string{
		"SET app.agent_id='x'",
		"set app.agent_id='x'",
		"SET\tapp.agent_id='x'",
		"/* /* */ */ SET app.agent_id='x'",
		"-- x\nSET app.agent_id='x'",
		"ALTER\tROLE agent_runtime SET row_security=off",
		"CREATE\nTABLE scratch.x(y int)",
		"DROP TABLE swarmpg.memory",
		"DO $$ BEGIN END $$",
		"COPY x FROM PROGRAM 'id'",
		"TRUNCATE swarmpg.events",
		"LOCK TABLE swarmpg.memory",
		"DECLARE c CURSOR WITH HOLD FOR SELECT 1",
		"FETCH ALL FROM c",
		"GRANT ALL ON swarmpg.memory TO public",
	}
	for _, s := range bad {
		if err := guard(s, a); err == nil {
			t.Errorf("guard allowed forbidden statement: %q", s)
		}
	}
}

func TestGuardAllowsLegitQueries(t *testing.T) {
	a := &Agent{}
	ok := []string{
		"SELECT * FROM swarmpg.memory",
		"WITH t AS (SELECT 1) SELECT * FROM t",
		"INSERT INTO swarmpg.documents(agent_id,org_id,collection,body) VALUES(swarmpg.current_agent(),swarmpg.current_org(),'c','{}')",
		"UPDATE swarmpg.memory SET value='{}' WHERE key='k'",
		"DELETE FROM swarmpg.memory WHERE key='k'",
		"EXPLAIN SELECT 1",
		"VALUES (1),(2)",
		"TABLE swarmpg.memory",
	}
	for _, s := range ok {
		if err := guard(s, a); err != nil {
			t.Errorf("guard rejected legit statement %q: %v", s, err)
		}
	}
}

func TestReadOnlyAgentBlocksWrites(t *testing.T) {
	ro := &Agent{ReadOnly: true}
	if err := guard("INSERT INTO swarmpg.memory(key) VALUES('k')", ro); err == nil {
		t.Error("read-only agent allowed INSERT")
	}
	if err := guard("SELECT 1", ro); err != nil {
		t.Errorf("read-only agent blocked SELECT: %v", err)
	}
}

func TestNormalizeParamPreservesBigInt(t *testing.T) {
	// 9007199254740993 = 2^53 + 1, not representable as float64.
	got := normalizeParam(json.Number("9007199254740993"))
	if iv, ok := got.(int64); !ok || iv != 9007199254740993 {
		t.Errorf("big int lost precision: got %#v", got)
	}
}
