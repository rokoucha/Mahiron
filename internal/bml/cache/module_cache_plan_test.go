package cache

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// Module payloads are stored inline, so a plan that scans data_broadcast_modules
// or data_broadcast_resources reads the blob overflow pages of the whole cache.
// That turned every prune - one per startup and one per stored module - into a
// full read of a cache that is allowed to grow to a gigabyte.
func TestSQLitePruneStatementsAvoidTableScans(t *testing.T) {
	store, err := NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	conn, err := store.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(t.Context(), pruneCreateCandidates); err != nil {
		t.Fatal(err)
	}

	for _, statement := range []struct {
		name  string
		query string
		args  []any
	}{
		{"stored bytes", pruneStoredBytesQuery, nil},
		{"collect batch", pruneCollectBatch, []any{moduleCachePruneBatchSize, moduleCachePruneBatchBytes, 0, 0}},
		{"delete resources", pruneDeleteResources, nil},
		{"delete modules", pruneDeleteModules, nil},
	} {
		t.Run(statement.name, func(t *testing.T) {
			plan := queryPlan(t, conn, statement.query, statement.args...)
			for _, table := range []string{"data_broadcast_modules", "data_broadcast_resources"} {
				for _, line := range strings.Split(plan, "\n") {
					if !strings.Contains(line, "SCAN") || !strings.Contains(line, table) {
						continue
					}
					if strings.Contains(line, "COVERING INDEX") {
						continue
					}
					t.Errorf("statement reads %s without a covering index:\n%s", table, plan)
				}
			}
		})
	}
}

func queryPlan(t *testing.T, conn *sql.Conn, query string, args ...any) string {
	t.Helper()
	rows, err := conn.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	lines := []string{}
	for rows.Next() {
		var id, parent, notUsed int64
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(lines, "\n")
}
