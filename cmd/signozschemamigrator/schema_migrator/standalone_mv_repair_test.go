package schemamigrator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var traces = SignozTracesDB

func storageTable(db, name, engine string) signozTable {
	return signozTable{
		Database:         db,
		Name:             name,
		Engine:           engine,
		CreateTableQuery: fmt.Sprintf("CREATE TABLE %s.%s (`x` String) ENGINE = %s ORDER BY x", db, name, engine),
	}
}

func aliasView(db, name, source string) signozTable {
	return signozTable{
		Database:         db,
		Name:             name,
		Engine:           "View",
		CreateTableQuery: fmt.Sprintf("CREATE VIEW %s.%s (`x` String) AS SELECT * FROM %s", db, name, source),
		AsSelect:         "SELECT * FROM " + source,
	}
}

func materializedView(db, name, target, body string) signozTable {
	return signozTable{
		Database: db,
		Name:     name,
		Engine:   "MaterializedView",
		// Formatted the way ClickHouse returns it: TO target, then the column list.
		CreateTableQuery: fmt.Sprintf("CREATE MATERIALIZED VIEW %s.%s TO %s (`x` String, `y` DateTime64(9)) AS %s", db, name, target, body),
		AsSelect:         body,
	}
}

func catalogOf(tables ...signozTable) map[string]signozTable {
	out := map[string]signozTable{}
	for _, t := range tables {
		out[t.fqName()] = t
	}
	return out
}

func expectedIn(db string) []expectedMV {
	var out []expectedMV
	for _, e := range expectedMVs().Present {
		if e.Database == db {
			out = append(out, e)
		}
	}
	return out
}

func expectedTraces() []expectedMV { return expectedIn(traces) }

// tracesOnly expects the trace MVs and honours every migration's drops.
func tracesOnly() mvExpectations {
	return mvExpectations{Present: expectedTraces(), Dropped: expectedMVs().Dropped}
}

func expectedByName(t *testing.T, name string) expectedMV {
	t.Helper()
	for _, e := range expectedMVs().Present {
		if e.Name == name {
			return e
		}
	}
	t.Fatalf("no expected MV %s", name)
	return expectedMV{}
}

// storageFor is where each trace migration's DestTable really lives in the
// fork's mixed layout: squashed MV targets keep storage under the base name,
// the others under distributed_*.
var storageFor = map[string]string{
	"trace_summary":               traces + ".distributed_trace_summary",
	"top_level_operations":        traces + ".distributed_top_level_operations",
	"usage_explorer":              traces + ".usage_explorer",
	"dependency_graph_minutes_v2": traces + ".dependency_graph_minutes_v2",
}

// traceCatalog is the storage and alias layout of a real standalone
// deployment's signoz_traces, without any MVs.
func traceCatalog() map[string]signozTable {
	return catalogOf(
		storageTable(traces, "distributed_signoz_index_v3", "SharedMergeTree"),
		aliasView(traces, "signoz_index_v3", traces+".distributed_signoz_index_v3"),
		storageTable(traces, "distributed_trace_summary", "SharedAggregatingMergeTree"),
		aliasView(traces, "trace_summary", traces+".distributed_trace_summary"),
		storageTable(traces, "distributed_top_level_operations", "SharedReplacingMergeTree"),
		aliasView(traces, "top_level_operations", traces+".distributed_top_level_operations"),
		storageTable(traces, "usage_explorer", "SharedSummingMergeTree"),
		aliasView(traces, "distributed_usage_explorer", traces+".usage_explorer"),
		storageTable(traces, "dependency_graph_minutes_v2", "SharedAggregatingMergeTree"),
		aliasView(traces, "distributed_dependency_graph_minutes_v2", traces+".dependency_graph_minutes_v2"),
	)
}

func correctBody(e expectedMV) string {
	return strings.ReplaceAll(e.Query, traces+".signoz_index_v3", traces+".distributed_signoz_index_v3")
}

// healthyCatalog is the traces layout plus every expected trace MV, correctly wired.
func healthyCatalog(t *testing.T) map[string]signozTable {
	t.Helper()
	tables := traceCatalog()
	for _, e := range expectedTraces() {
		target, ok := storageFor[e.DestTable]
		require.True(t, ok, "no storage fixture for %s", e.DestTable)
		tables[e.fqName()] = materializedView(traces, e.Name, target, correctBody(e))
	}
	return tables
}

func brokenUsageCatalog(t *testing.T) map[string]signozTable {
	t.Helper()
	tables := healthyCatalog(t)
	e := expectedByName(t, "usage_explorer_mv")
	tables[e.fqName()] = materializedView(traces, e.Name, traces+".distributed_usage_explorer", correctBody(e))
	return tables
}

func expectedNames(db string) []string {
	var names []string
	for _, e := range expectedIn(db) {
		names = append(names, e.Name)
	}
	return names
}

func TestExpectedMVs(t *testing.T) {
	require.Equal(t, []string{
		"dependency_graph_minutes_db_calls_mv_v2",
		"dependency_graph_minutes_messaging_calls_mv_v2",
		"dependency_graph_minutes_service_calls_mv_v2",
		"root_operations",
		"sub_root_operations",
		"trace_summary_mv",
		"usage_explorer_mv",
	}, expectedNames(SignozTracesDB), "durationSortMV and the v1 dependency graph MVs are dropped by later migrations")

	require.Equal(t, []string{
		"samples_v4_agg_30m_mv",
		"samples_v4_agg_5m_mv",
		"time_series_v4_1day_mv",
		"time_series_v4_1week_mv",
		"time_series_v4_6hrs_mv",
	}, expectedNames(SignozMetricsDB), "the *_separate_attrs MVs are dropped by migration 1005")

	require.Empty(t, expectedNames(SignozLogsDB), "logs migration 1000 drops every logs MV")
	require.Equal(t, []string{"samples_agg_1d_mv"}, expectedNames(SignozMeterDB))
	require.Empty(t, expectedNames(SignozMetadataDB))
	require.Empty(t, expectedNames(SignozAnalyticsDB))

	dropped := expectedMVs().Dropped
	for _, name := range []string{
		"signoz_traces.durationSortMV",
		"signoz_metrics.time_series_v4_6hrs_mv_separate_attrs",
		"signoz_logs.attribute_keys_string_final_mv",
	} {
		require.True(t, dropped[name], "%s is dropped by a migration", name)
	}
	require.False(t, dropped["signoz_traces.trace_summary_mv"])

	for _, e := range expectedMVs().Present {
		require.False(t, strings.HasSuffix(e.Query, ";"), "%s keeps a trailing semicolon", e.fqName())
		require.NotEmpty(t, e.DestTable, e.fqName())
	}
	for _, e := range expectedTraces() {
		require.NotContains(t, e.Query, traces+".signoz_index_v2", "%s still has a pre-v3 query", e.Name)
	}

	require.Equal(t, "dependency_graph_minutes_v2", expectedByName(t, "dependency_graph_minutes_db_calls_mv_v2").DestTable)
	require.Equal(t, "top_level_operations", expectedByName(t, "root_operations").DestTable)
	require.Equal(t, "trace_summary", expectedByName(t, "trace_summary_mv").DestTable)
	require.Equal(t, "usage_explorer", expectedByName(t, "usage_explorer_mv").DestTable)

	// The last migration that touched each MV wins.
	require.Contains(t, expectedByName(t, "usage_explorer_mv").Query, "resource_string_service$$name", "migration 1009")
	require.Contains(t, expectedByName(t, "sub_root_operations").Query, "B.span_id != ''", "migration 1007")
	require.Contains(t, expectedByName(t, "dependency_graph_minutes_service_calls_mv_v2").Query, "B.span_id != ''", "migration 1007")
}

func TestReplayMaterializedViews(t *testing.T) {
	records := []SchemaMigrationRecord{
		{MigrationID: 1, UpItems: []Operation{
			CreateMaterializedViewOperation{Database: "db", ViewName: "a", DestTable: "t", Query: "SELECT 1 FROM db.s;"},
			&CreateMaterializedViewOperation{Database: "db", ViewName: "b", DestTable: "t", Query: "SELECT 2 FROM db.s"},
			CreateMaterializedViewOperation{Database: "other", ViewName: "c", DestTable: "t", Query: "SELECT 3 FROM other.s"},
		}},
		{MigrationID: 2, UpItems: []Operation{
			ModifyQueryMaterializedViewOperation{Database: "db", ViewName: "a", Query: "SELECT 10 FROM db.s"},
			&DropTableOperation{Database: "db", Table: "b"},
			ModifyQueryMaterializedViewOperation{Database: "db", ViewName: "never_created", Query: "SELECT 0 FROM db.s"},
		}},
	}
	got := replayMaterializedViews("db", records)
	require.Equal(t, []expectedMV{{Database: "db", Name: "a", DestTable: "t", Query: "SELECT 10 FROM db.s"}}, got.Present)
	require.Equal(t, map[string]bool{"db.b": true}, got.Dropped)

	// A later CREATE brings a dropped MV back.
	recreated := append(records, SchemaMigrationRecord{MigrationID: 3, UpItems: []Operation{
		CreateMaterializedViewOperation{Database: "db", ViewName: "b", DestTable: "t", Query: "SELECT 3 FROM db.s"},
	}})
	got = replayMaterializedViews("db", recreated)
	require.Len(t, got.Present, 2)
	require.Empty(t, got.Dropped)

	// Migrations standalone mode never runs contribute nothing.
	skipped := []SchemaMigrationRecord{{MigrationID: 1008, UpItems: []Operation{
		CreateMaterializedViewOperation{Database: traces, ViewName: "x", DestTable: "t", Query: "SELECT 1 FROM signoz_traces.s"},
	}}}
	require.Empty(t, replayMaterializedViews(traces, skipped).Present)
}

func TestParseMVTarget(t *testing.T) {
	testCases := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "column-list-after-target",
			query: "CREATE MATERIALIZED VIEW db.mv TO db.dest (`a` String, `b` DateTime64(9)) AS SELECT a, b FROM db.src",
			want:  "db.dest",
		},
		{
			name:  "no-column-list",
			query: "CREATE MATERIALIZED VIEW db.mv TO db.dest AS SELECT a FROM db.src",
			want:  "db.dest",
		},
		{
			name:  "backquoted-target",
			query: "CREATE MATERIALIZED VIEW db.mv TO `db`.`dest` AS SELECT a FROM db.src",
			want:  "db.dest",
		},
		{
			name:  "unqualified-target-resolves-in-the-mv-database",
			query: "CREATE MATERIALIZED VIEW db.mv TO dest AS SELECT a FROM db.src",
			want:  "db.dest",
		},
		{
			name:  "a-TO-inside-the-select-is-not-the-target",
			query: "CREATE MATERIALIZED VIEW db.mv ENGINE = MergeTree ORDER BY a AS SELECT a FROM db.src WHERE b != ' TO db.nope'",
			want:  "",
		},
		{
			name:  "mv-that-owns-its-storage",
			query: "CREATE MATERIALIZED VIEW db.mv (`a` String) ENGINE = SharedMergeTree ORDER BY a AS SELECT a FROM db.src",
			want:  "",
		},
		{
			name:  "a-ttl-move-to-volume-is-not-a-target",
			query: "CREATE MATERIALIZED VIEW db.mv (`a` DateTime) ENGINE = SharedMergeTree ORDER BY a TTL a + toIntervalDay(1) TO VOLUME 'cold' AS SELECT a FROM db.src",
			want:  "",
		},
		{
			name:  "refreshable-mvs-are-left-alone",
			query: "CREATE MATERIALIZED VIEW db.mv REFRESH EVERY 1 HOUR TO db.dest AS SELECT a FROM db.view_on_purpose",
			want:  "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, parseMVTarget("db", tc.query))
		})
	}
}

func TestMVSelectBody(t *testing.T) {
	const full = "SELECT trace_id, min(timestamp) AS start FROM db.src GROUP BY trace_id"
	const ctq = "CREATE MATERIALIZED VIEW db.mv TO db.dest (`trace_id` String) AS " + full

	testCases := []struct {
		name     string
		ctq      string
		asSelect string
		want     string
		wantOK   bool
	}{
		{name: "create-table-query-is-authoritative", ctq: ctq, asSelect: full, want: full, wantOK: true},
		{
			// A cut that still contains FROM must not win over the full DDL.
			name:     "truncated-as-select-with-a-from-is-ignored",
			ctq:      ctq,
			asSelect: "SELECT trace_id FROM db.src",
			want:     full,
			wantOK:   true,
		},
		{name: "falls-back-to-as-select", ctq: "", asSelect: full, want: full, wantOK: true},
		{
			name:     "a-fragment-without-from-is-rejected",
			asSelect: "SELECT trace_id, min(timestamp) AS start, toUInt64(count())",
			wantOK:   false,
		},
		{name: "nothing-to-read", wantOK: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := mvSelectBody(tc.ctq, tc.asSelect)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestReplaceTableRefs(t *testing.T) {
	repl := map[string]string{
		"db.usage_explorer": "db.distributed_usage_explorer",
		"db.index_v3":       "db.distributed_index_v3",
	}
	testCases := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "rewrites-every-whole-reference",
			query: "SELECT 1 FROM db.index_v3 AS A, db.index_v3 AS B",
			want:  "SELECT 1 FROM db.distributed_index_v3 AS A, db.distributed_index_v3 AS B",
		},
		{
			name:  "a-longer-name-sharing-the-prefix-is-left-alone",
			query: "SELECT 1 FROM db.usage_explorer_mv",
			want:  "SELECT 1 FROM db.usage_explorer_mv",
		},
		{
			name:  "the-replacement-is-not-rewritten-again",
			query: "SELECT 1 FROM db.distributed_index_v3",
			want:  "SELECT 1 FROM db.distributed_index_v3",
		},
		{
			name:  "a-reference-qualified-by-something-else-is-left-alone",
			query: "SELECT 1 FROM cluster.db.index_v3",
			want:  "SELECT 1 FROM cluster.db.index_v3",
		},
		{
			name:  "references-at-the-edges-and-in-parentheses",
			query: "db.index_v3 JOIN (SELECT * FROM db.index_v3)",
			want:  "db.distributed_index_v3 JOIN (SELECT * FROM db.distributed_index_v3)",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, replaceTableRefs(tc.query, repl))
		})
	}
}

func TestBuildViewAliases(t *testing.T) {
	tables := traceCatalog()
	filtered := signozTable{
		Database:         traces,
		Name:             "errors_only",
		Engine:           "View",
		CreateTableQuery: "CREATE VIEW signoz_traces.errors_only AS SELECT * FROM signoz_traces.distributed_signoz_index_v3 WHERE has_error",
	}
	viewOfView := aliasView(traces, "alias_of_alias", traces+".trace_summary")
	tables[filtered.fqName()] = filtered
	tables[viewOfView.fqName()] = viewOfView

	require.Equal(t, map[string]string{
		// Storage under distributed_*.
		traces + ".signoz_index_v3":      traces + ".distributed_signoz_index_v3",
		traces + ".trace_summary":        traces + ".distributed_trace_summary",
		traces + ".top_level_operations": traces + ".distributed_top_level_operations",
		// Storage under the base name.
		traces + ".distributed_usage_explorer":              traces + ".usage_explorer",
		traces + ".distributed_dependency_graph_minutes_v2": traces + ".dependency_graph_minutes_v2",
	}, buildViewAliases(tables), "a filtered view and a view over a view are not storage aliases")
}

func TestPlanStandaloneMVs(t *testing.T) {
	t.Run("a-healthy-catalog-needs-nothing", func(t *testing.T) {
		require.Empty(t, planStandaloneMVs(healthyCatalog(t), tracesOnly()).Actions)
	})

	t.Run("an-mv-writing-to-an-alias-view-is-rebuilt-onto-storage", func(t *testing.T) {
		// The incident: usage_explorer_mv pointed at distributed_usage_explorer,
		// which in this layout is the VIEW, not the table.
		plan := planStandaloneMVs(brokenUsageCatalog(t), tracesOnly())
		e := expectedByName(t, "usage_explorer_mv")
		require.Equal(t, []mvAction{{
			Kind: mvActionRebuild, Database: traces, Name: e.Name,
			Target: traces + ".usage_explorer", Select: correctBody(e),
		}}, plan.Actions)
		require.Empty(t, validateMaterializedViews(plan.Simulated, tracesOnly()))
	})

	t.Run("an-mv-reading-an-alias-view-gets-its-query-modified", func(t *testing.T) {
		tables := healthyCatalog(t)
		e := expectedByName(t, "root_operations")
		tables[e.fqName()] = materializedView(traces, e.Name, traces+".distributed_top_level_operations", e.Query)

		plan := planStandaloneMVs(tables, tracesOnly())
		require.Len(t, plan.Actions, 1)
		require.Equal(t, mvActionModifyQuery, plan.Actions[0].Kind)
		require.Equal(t, correctBody(e), plan.Actions[0].Select)
		require.Empty(t, validateMaterializedViews(plan.Simulated, tracesOnly()))
	})

	t.Run("missing-mvs-are-recreated-against-storage", func(t *testing.T) {
		plan := planStandaloneMVs(traceCatalog(), tracesOnly())
		require.Len(t, plan.Actions, len(expectedTraces()))

		targets := map[string]string{}
		for _, a := range plan.Actions {
			require.Equal(t, mvActionRecreate, a.Kind)
			require.NotContains(t, a.Select, traces+".signoz_index_v3", "%s must read storage", a.Name)
			targets[a.Name] = a.Target
		}
		require.Equal(t, traces+".distributed_trace_summary", targets["trace_summary_mv"])
		require.Equal(t, traces+".distributed_top_level_operations", targets["root_operations"])
		require.Equal(t, traces+".usage_explorer", targets["usage_explorer_mv"])
		require.Equal(t, traces+".dependency_graph_minutes_v2", targets["dependency_graph_minutes_db_calls_mv_v2"])
		require.Empty(t, validateMaterializedViews(plan.Simulated, tracesOnly()))
	})

	t.Run("a-missing-mv-is-not-recreated-over-a-non-storage-target", func(t *testing.T) {
		tables := healthyCatalog(t)
		e := expectedByName(t, "usage_explorer_mv")
		delete(tables, e.fqName())
		// Storage gone, only a non-alias VIEW left under the target name.
		delete(tables, traces+".usage_explorer")
		odd := signozTable{
			Database: traces, Name: "usage_explorer", Engine: "View",
			CreateTableQuery: "CREATE VIEW signoz_traces.usage_explorer AS SELECT 1 AS count",
		}
		tables[odd.fqName()] = odd

		plan := planStandaloneMVs(tables, tracesOnly())
		require.Len(t, plan.Actions, 1)
		require.Equal(t, mvActionUnresolvable, plan.Actions[0].Kind)
		require.Contains(t, plan.Actions[0].String(), "is not a storage table")
		require.Contains(t, validateMaterializedViews(plan.Simulated, tracesOnly()), e.fqName()+" is missing")
	})

	t.Run("an-mv-that-owns-its-storage-is-never-touched", func(t *testing.T) {
		tables := healthyCatalog(t)
		inline := signozTable{
			Database:         traces,
			Name:             "custom_inline_mv",
			Engine:           "MaterializedView",
			CreateTableQuery: "CREATE MATERIALIZED VIEW signoz_traces.custom_inline_mv (`n` UInt64) ENGINE = SharedMergeTree ORDER BY n AS SELECT count() AS n FROM signoz_traces.signoz_index_v3",
		}
		tables[inline.fqName()] = inline

		require.Empty(t, planStandaloneMVs(tables, tracesOnly()).Actions)
	})

	t.Run("an-mv-a-migration-drops-is-left-alone-and-not-required", func(t *testing.T) {
		// durationSortMV is dropped by traces migration 1001, which runs in the
		// async phase; after sync alone it may still exist, even broken.
		tables := healthyCatalog(t)
		legacy := materializedView(traces, "durationSortMV", traces+".distributed_usage_explorer", "SELECT 1 FROM signoz_traces.signoz_index_v3")
		tables[legacy.fqName()] = legacy

		plan := planStandaloneMVs(tables, tracesOnly())
		require.Empty(t, plan.Actions)
		require.Empty(t, validateMaterializedViews(plan.Simulated, tracesOnly()))
	})

	t.Run("reproduces-the-numia-logs-metrics-preflight", func(t *testing.T) {
		// signoz_metrics on numia-logs on 2026-09-17: storage and aliases in
		// place, all five rollup MVs gone. check-mvs planned exactly these.
		metrics := SignozMetricsDB
		tables := catalogOf(
			storageTable(metrics, "distributed_samples_v4", "SharedMergeTree"),
			aliasView(metrics, "samples_v4", metrics+".distributed_samples_v4"),
			storageTable(metrics, "samples_v4_agg_5m", "SharedAggregatingMergeTree"),
			aliasView(metrics, "distributed_samples_v4_agg_5m", metrics+".samples_v4_agg_5m"),
			storageTable(metrics, "samples_v4_agg_30m", "SharedAggregatingMergeTree"),
			aliasView(metrics, "distributed_samples_v4_agg_30m", metrics+".samples_v4_agg_30m"),
			storageTable(metrics, "distributed_time_series_v4", "SharedReplacingMergeTree"),
			aliasView(metrics, "time_series_v4", metrics+".distributed_time_series_v4"),
			storageTable(metrics, "distributed_time_series_v4_6hrs", "SharedReplacingMergeTree"),
			aliasView(metrics, "time_series_v4_6hrs", metrics+".distributed_time_series_v4_6hrs"),
			storageTable(metrics, "distributed_time_series_v4_1day", "SharedReplacingMergeTree"),
			aliasView(metrics, "time_series_v4_1day", metrics+".distributed_time_series_v4_1day"),
			storageTable(metrics, "distributed_time_series_v4_1week", "SharedReplacingMergeTree"),
			aliasView(metrics, "time_series_v4_1week", metrics+".distributed_time_series_v4_1week"),
		)
		expected := mvExpectations{Present: expectedIn(metrics), Dropped: expectedMVs().Dropped}

		plan := planStandaloneMVs(tables, expected)
		var actions []string
		for _, a := range plan.Actions {
			actions = append(actions, a.String())
		}
		require.Equal(t, []string{
			"recreate-missing signoz_metrics.samples_v4_agg_30m_mv (TO signoz_metrics.samples_v4_agg_30m)",
			"recreate-missing signoz_metrics.samples_v4_agg_5m_mv (TO signoz_metrics.samples_v4_agg_5m)",
			"recreate-missing signoz_metrics.time_series_v4_1day_mv (TO signoz_metrics.distributed_time_series_v4_1day)",
			"recreate-missing signoz_metrics.time_series_v4_1week_mv (TO signoz_metrics.distributed_time_series_v4_1week)",
			"recreate-missing signoz_metrics.time_series_v4_6hrs_mv (TO signoz_metrics.distributed_time_series_v4_6hrs)",
		}, actions)
		// Every recreated body reads storage, including the chained rollups.
		require.Empty(t, validateMaterializedViews(plan.Simulated, expected))
	})

	t.Run("an-unexpected-mv-is-never-dropped", func(t *testing.T) {
		tables := healthyCatalog(t)
		extra := materializedView(traces, "someone_elses_mv", traces+".usage_explorer", "SELECT 1 FROM signoz_traces.distributed_signoz_index_v3")
		tables[extra.fqName()] = extra

		plan := planStandaloneMVs(tables, tracesOnly())
		require.Empty(t, plan.Actions)
		require.Contains(t, plan.Simulated, extra.fqName())
	})

	t.Run("an-unreadable-broken-mv-is-reported-not-rebuilt", func(t *testing.T) {
		tables := healthyCatalog(t)
		e := expectedByName(t, "usage_explorer_mv")
		tables[e.fqName()] = signozTable{
			Database:         traces,
			Name:             e.Name,
			Engine:           "MaterializedView",
			CreateTableQuery: "CREATE MATERIALIZED VIEW signoz_traces.usage_explorer_mv TO signoz_traces.distributed_usage_explorer",
			AsSelect:         "SELECT toStartOfHour(timestamp) AS timestamp, count()",
		}

		plan := planStandaloneMVs(tables, tracesOnly())
		require.Len(t, plan.Actions, 1)
		require.Equal(t, mvActionUnresolvable, plan.Actions[0].Kind)
		require.NotEmpty(t, validateMaterializedViews(plan.Simulated, tracesOnly()), "sync must fail on it")
	})

	t.Run("metrics-layout-with-chained-aggregates", func(t *testing.T) {
		metrics := SignozMetricsDB
		tables := catalogOf(
			// The collector writes to distributed_samples_v4, so that is storage.
			storageTable(metrics, "distributed_samples_v4", "SharedMergeTree"),
			aliasView(metrics, "samples_v4", metrics+".distributed_samples_v4"),
			// Squashed MV targets keep storage under the base name.
			storageTable(metrics, "samples_v4_agg_5m", "SharedAggregatingMergeTree"),
			aliasView(metrics, "distributed_samples_v4_agg_5m", metrics+".samples_v4_agg_5m"),
			storageTable(metrics, "samples_v4_agg_30m", "SharedAggregatingMergeTree"),
			aliasView(metrics, "distributed_samples_v4_agg_30m", metrics+".samples_v4_agg_30m"),
			// 5m reads raw samples through the VIEW (never fires); 30m reads 5m storage (fine).
			materializedView(metrics, "samples_v4_agg_5m_mv", metrics+".samples_v4_agg_5m",
				"SELECT fingerprint, sum(value) AS sum FROM signoz_metrics.samples_v4 GROUP BY fingerprint"),
			materializedView(metrics, "samples_v4_agg_30m_mv", metrics+".samples_v4_agg_30m",
				"SELECT fingerprint, sum(sum) AS sum FROM signoz_metrics.samples_v4_agg_5m GROUP BY fingerprint"),
		)

		plan := planStandaloneMVs(tables, mvExpectations{})
		require.Equal(t, []mvAction{{
			Kind: mvActionModifyQuery, Database: metrics, Name: "samples_v4_agg_5m_mv",
			Target: metrics + ".samples_v4_agg_5m",
			Select: "SELECT fingerprint, sum(value) AS sum FROM signoz_metrics.distributed_samples_v4 GROUP BY fingerprint",
		}}, plan.Actions)
		require.Empty(t, validateMaterializedViews(plan.Simulated, mvExpectations{}))
	})
}

func TestValidateMaterializedViews(t *testing.T) {
	require.Empty(t, validateMaterializedViews(healthyCatalog(t), tracesOnly()))

	tables := healthyCatalog(t)
	summary := expectedByName(t, "trace_summary_mv")
	usage := expectedByName(t, "usage_explorer_mv")
	root := expectedByName(t, "root_operations")
	delete(tables, summary.fqName())
	tables[usage.fqName()] = materializedView(traces, usage.Name, traces+".distributed_usage_explorer", correctBody(usage))
	tables[root.fqName()] = materializedView(traces, root.Name, traces+".distributed_top_level_operations", root.Query)
	ghost := materializedView(traces, "ghost_mv", traces+".nowhere", "SELECT 1 FROM signoz_traces.distributed_signoz_index_v3")
	tables[ghost.fqName()] = ghost

	require.ElementsMatch(t, []string{
		"signoz_traces.trace_summary_mv is missing",
		"signoz_traces.usage_explorer_mv writes to signoz_traces.distributed_usage_explorer (View), not a storage table",
		"signoz_traces.root_operations reads from an alias VIEW, so it never fires",
		"signoz_traces.ghost_mv writes to signoz_traces.nowhere, which does not exist",
	}, validateMaterializedViews(tables, tracesOnly()))
}

// fakeRows serves one catalog snapshot to loadSignozTables.
type fakeRows struct {
	driver.Rows
	data []signozTable
	pos  int
}

func (r *fakeRows) Next() bool { r.pos++; return r.pos <= len(r.data) }

func (r *fakeRows) Scan(dest ...any) error {
	t := r.data[r.pos-1]
	for i, v := range []string{t.Database, t.Name, t.Engine, t.CreateTableQuery, t.AsSelect} {
		*(dest[i].(*string)) = v
	}
	return nil
}

func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return nil }

// fakeConn returns one catalog per Query call (repeating the last one) and
// records every Exec, failing the ones failExec selects.
type fakeConn struct {
	clickhouse.Conn
	catalogs  []map[string]signozTable
	queries   int
	describes []string
	execs     []string
	failExec  func(sql string) bool
	// failDescribe makes DESCRIBE of a SELECT containing this text fail.
	failDescribe string
}

func (c *fakeConn) Query(_ context.Context, sql string, _ ...any) (driver.Rows, error) {
	if strings.HasPrefix(sql, "DESCRIBE (") {
		c.describes = append(c.describes, sql)
		if c.failDescribe != "" && strings.Contains(sql, c.failDescribe) {
			return nil, errors.New("Unknown expression identifier")
		}
		return &fakeRows{data: []signozTable{{}}}, nil
	}
	idx := min(c.queries, len(c.catalogs)-1)
	c.queries++
	var data []signozTable
	for _, k := range sortedKeys(c.catalogs[idx]) {
		data = append(data, c.catalogs[idx][k])
	}
	return &fakeRows{data: data}, nil
}

func (c *fakeConn) Exec(_ context.Context, sql string, _ ...any) error {
	c.execs = append(c.execs, sql)
	if c.failExec != nil && c.failExec(sql) {
		return errors.New("boom")
	}
	return nil
}

func newTestManager(conn *fakeConn) *MigrationManager {
	return &MigrationManager{conn: conn, logger: zap.NewNop(), mvExpected: tracesOnly}
}

// statementHeads keeps verb and object of each statement, without the SELECT.
func statementHeads(execs []string) []string {
	out := make([]string, 0, len(execs))
	for _, s := range execs {
		if idx := strings.Index(s, " AS "); idx != -1 {
			s = s[:idx]
		}
		out = append(out, s)
	}
	return out
}

func TestRecreateMaterializedViewsForStandalone(t *testing.T) {
	ctx := context.Background()
	const mv = "signoz_traces.usage_explorer_mv"

	t.Run("a-healthy-catalog-runs-no-statement", func(t *testing.T) {
		conn := &fakeConn{catalogs: []map[string]signozTable{healthyCatalog(t)}}
		require.NoError(t, newTestManager(conn).RecreateMaterializedViewsForStandalone(ctx))
		require.Empty(t, conn.execs)
		require.Equal(t, 2, conn.queries, "one read to plan, one to validate")
	})

	t.Run("cluster-mode-is-left-alone", func(t *testing.T) {
		conn := &fakeConn{catalogs: []map[string]signozTable{brokenUsageCatalog(t)}}
		m := newTestManager(conn)
		m.clusterName = "cluster"
		require.NoError(t, m.RecreateMaterializedViewsForStandalone(ctx))
		require.Zero(t, conn.queries)
	})

	t.Run("a-broken-target-is-dropped-and-recreated-on-storage", func(t *testing.T) {
		conn := &fakeConn{catalogs: []map[string]signozTable{brokenUsageCatalog(t), healthyCatalog(t)}}
		require.NoError(t, newTestManager(conn).RecreateMaterializedViewsForStandalone(ctx))
		require.Equal(t, []string{
			"DROP VIEW IF EXISTS " + mv,
			"CREATE MATERIALIZED VIEW IF NOT EXISTS " + mv + " TO signoz_traces.usage_explorer",
		}, statementHeads(conn.execs))
	})

	t.Run("a-failed-create-fails-sync", func(t *testing.T) {
		conn := &fakeConn{
			catalogs: []map[string]signozTable{brokenUsageCatalog(t)},
			failExec: func(sql string) bool { return strings.HasPrefix(sql, "CREATE") },
		}
		err := newTestManager(conn).RecreateMaterializedViewsForStandalone(ctx)
		require.ErrorContains(t, err, "usage_explorer_mv writes to signoz_traces.distributed_usage_explorer (View)")
		require.ErrorContains(t, err, "boom", "the failed step is part of the error")
	})

	t.Run("a-select-that-does-not-analyze-changes-nothing", func(t *testing.T) {
		conn := &fakeConn{catalogs: []map[string]signozTable{brokenUsageCatalog(t)}, failDescribe: "service_name"}
		err := newTestManager(conn).RecreateMaterializedViewsForStandalone(ctx)
		require.ErrorContains(t, err, "the new SELECT does not analyze, nothing was changed")
		require.Empty(t, conn.execs, "not even the DROP of the rebuild runs")
	})

	t.Run("a-from-only-fix-never-drops", func(t *testing.T) {
		before := healthyCatalog(t)
		e := expectedByName(t, "root_operations")
		before[e.fqName()] = materializedView(traces, e.Name, traces+".distributed_top_level_operations", e.Query)

		conn := &fakeConn{catalogs: []map[string]signozTable{before, healthyCatalog(t)}}
		require.NoError(t, newTestManager(conn).RecreateMaterializedViewsForStandalone(ctx))
		require.Equal(t, []string{"ALTER TABLE signoz_traces.root_operations MODIFY QUERY " + correctBody(e)}, conn.execs)
	})

	t.Run("missing-mvs-are-recreated", func(t *testing.T) {
		conn := &fakeConn{catalogs: []map[string]signozTable{traceCatalog(), healthyCatalog(t)}}
		require.NoError(t, newTestManager(conn).RecreateMaterializedViewsForStandalone(ctx))

		created := statementHeads(conn.execs)
		sort.Strings(created)
		require.Equal(t, []string{
			"CREATE MATERIALIZED VIEW IF NOT EXISTS signoz_traces.dependency_graph_minutes_db_calls_mv_v2 TO signoz_traces.dependency_graph_minutes_v2",
			"CREATE MATERIALIZED VIEW IF NOT EXISTS signoz_traces.dependency_graph_minutes_messaging_calls_mv_v2 TO signoz_traces.dependency_graph_minutes_v2",
			"CREATE MATERIALIZED VIEW IF NOT EXISTS signoz_traces.dependency_graph_minutes_service_calls_mv_v2 TO signoz_traces.dependency_graph_minutes_v2",
			"CREATE MATERIALIZED VIEW IF NOT EXISTS signoz_traces.root_operations TO signoz_traces.distributed_top_level_operations",
			"CREATE MATERIALIZED VIEW IF NOT EXISTS signoz_traces.sub_root_operations TO signoz_traces.distributed_top_level_operations",
			"CREATE MATERIALIZED VIEW IF NOT EXISTS signoz_traces.trace_summary_mv TO signoz_traces.distributed_trace_summary",
			"CREATE MATERIALIZED VIEW IF NOT EXISTS signoz_traces.usage_explorer_mv TO signoz_traces.usage_explorer",
		}, created)
	})

	t.Run("the-gate-waits-for-ddl-to-become-visible", func(t *testing.T) {
		// Plan read, then two reads that do not see the change yet.
		broken := brokenUsageCatalog(t)
		conn := &fakeConn{catalogs: []map[string]signozTable{broken, broken, broken, healthyCatalog(t)}}
		require.NoError(t, newTestManager(conn).RecreateMaterializedViewsForStandalone(ctx))
		require.Equal(t, 4, conn.queries)
	})

	t.Run("the-gate-gives-up-after-its-attempts", func(t *testing.T) {
		conn := &fakeConn{catalogs: []map[string]signozTable{brokenUsageCatalog(t)}}
		require.Error(t, newTestManager(conn).RecreateMaterializedViewsForStandalone(ctx))
		require.Equal(t, 1+mvGateAttempts, conn.queries)
	})

	t.Run("warn-only-mode-reports-but-does-not-fail", func(t *testing.T) {
		conn := &fakeConn{
			catalogs: []map[string]signozTable{brokenUsageCatalog(t)},
			failExec: func(string) bool { return true },
		}
		m := newTestManager(conn)
		m.mvGateWarnOnly = true
		require.NoError(t, m.RecreateMaterializedViewsForStandalone(ctx))
	})
}

func TestCheckMaterializedViewsForStandalone(t *testing.T) {
	conn := &fakeConn{catalogs: []map[string]signozTable{brokenUsageCatalog(t)}}
	report, err := newTestManager(conn).CheckMaterializedViewsForStandalone(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{
		"rebuild signoz_traces.usage_explorer_mv (TO signoz_traces.usage_explorer)",
	}, report.Actions)
	require.Empty(t, report.Problems)
	require.Len(t, conn.describes, 1, "the new SELECT is analyzed")
	require.Empty(t, conn.execs, "the check must not change anything")
}

func TestCheckMaterializedViewsForStandaloneReportsABadSelect(t *testing.T) {
	conn := &fakeConn{catalogs: []map[string]signozTable{brokenUsageCatalog(t)}, failDescribe: "service_name"}
	report, err := newTestManager(conn).CheckMaterializedViewsForStandalone(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Problems, 1)
	require.Contains(t, report.Problems[0], "signoz_traces.usage_explorer_mv: the SELECT it would get does not analyze")
	require.Empty(t, conn.execs)
}
