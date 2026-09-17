package schemamigrator

// Materialized-view repair for standalone (SharedMergeTree) deployments.
//
// In standalone mode every logical table exists twice: once as storage and once
// as a plain alias VIEW (`CREATE VIEW x AS SELECT * FROM y`). Which name holds
// the storage depends on how the table was created — tables the collector
// writes to keep storage under `distributed_*`, while tables that squashed
// migrations create for MVs keep it under the base name. Both layouts coexist
// in one database, so nothing about a name tells you which one is the table.
//
// A materialized view is only correct if it both writes TO and reads FROM a
// storage table: an MV reading a VIEW never fires (inserts land in the storage
// table), and an MV writing to a VIEW fails every INSERT into its source. On
// ObsessionDB it is worse — an MV whose target is not SharedMergeTree is never
// registered in the shared metadata backend, and the table watcher's orphan
// reaper drops it on its next pass without any error reaching the client.
//
// So every decision is made against the live catalog (which name is a VIEW,
// which is storage), never against name conventions, and an MV that is already
// correct is never touched. The whole plan is computed up front by a pure
// function over the catalog, which is also what `check-mvs` reports.

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/SigNoz/signoz-otel-collector/constants"
	"go.uber.org/zap"
)

// mvGateAttempts is how many times the final check reads the catalog before
// failing, to ride out DDL that has not reached the node being read yet.
const mvGateAttempts = 5

// signozTable is the subset of system.tables the MV repair needs.
type signozTable struct {
	Database         string
	Name             string
	Engine           string
	CreateTableQuery string
	AsSelect         string
}

func (t signozTable) fqName() string { return t.Database + "." + t.Name }

func (t signozTable) isMaterializedView() bool { return t.Engine == "MaterializedView" }

// isStorageEngine reports whether a table can be an MV target. On ObsessionDB
// only SharedMergeTree targets get registered; every variant ends in MergeTree.
func isStorageEngine(engine string) bool {
	return strings.HasSuffix(engine, "MergeTree")
}

// expectedMV is the definition an MV should have once every migration ran.
type expectedMV struct {
	Database  string
	Name      string
	DestTable string // as written in the migration: bare name, same database
	Query     string
}

func (e expectedMV) fqName() string { return e.Database + "." + e.Name }

// expectedMVs replays, per database, the migration lists sync runs, in the
// order it runs them, and returns the MVs that must exist afterwards — each
// with the query of the last migration that touched it. Deriving this from the
// migrations keeps it right when upstream adds, modifies or drops an MV.
func expectedMVs() []expectedMV {
	logsMigrations := LogsMigrations
	if constants.EnableLogsMigrationsV2 {
		logsMigrations = LogsMigrationsV2
	}
	perDatabase := []struct {
		database string
		lists    [][]SchemaMigrationRecord
	}{
		{SignozTracesDB, [][]SchemaMigrationRecord{SquashedTracesMigrations, TracesMigrations}},
		{SignozMetricsDB, [][]SchemaMigrationRecord{SquashedMetricsMigrations, MetricsMigrations}},
		// Squashed logs migrations are not run; the custom retention set is.
		{SignozLogsDB, [][]SchemaMigrationRecord{CustomRetentionLogsMigrations, logsMigrations}},
		{SignozMetadataDB, [][]SchemaMigrationRecord{MetadataMigrations}},
		{SignozAnalyticsDB, [][]SchemaMigrationRecord{AnalyticsMigrations}},
		{SignozMeterDB, [][]SchemaMigrationRecord{MeterMigrations}},
	}

	var out []expectedMV
	for _, db := range perDatabase {
		var records []SchemaMigrationRecord
		for _, list := range db.lists {
			records = append(records, list...)
		}
		out = append(out, replayMaterializedViews(db.database, records)...)
	}
	return out
}

func (m *MigrationManager) expectedMaterializedViews() []expectedMV {
	if m.mvExpected != nil {
		return m.mvExpected()
	}
	return expectedMVs()
}

func replayMaterializedViews(database string, records []SchemaMigrationRecord) []expectedMV {
	byName := map[string]*expectedMV{}
	for _, record := range records {
		if isSkippedInStandaloneMode(database, record.MigrationID) {
			continue
		}
		for _, item := range record.UpItems {
			switch op := item.(type) {
			case CreateMaterializedViewOperation:
				replayCreate(byName, database, op)
			case *CreateMaterializedViewOperation:
				replayCreate(byName, database, *op)
			case ModifyQueryMaterializedViewOperation:
				replayModify(byName, database, op)
			case *ModifyQueryMaterializedViewOperation:
				replayModify(byName, database, *op)
			case DropTableOperation:
				replayDrop(byName, database, op)
			case *DropTableOperation:
				replayDrop(byName, database, *op)
			}
		}
	}

	out := make([]expectedMV, 0, len(byName))
	for _, mv := range byName {
		out = append(out, *mv)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func replayCreate(byName map[string]*expectedMV, database string, op CreateMaterializedViewOperation) {
	if op.Database != database {
		return
	}
	byName[op.ViewName] = &expectedMV{
		Database:  op.Database,
		Name:      op.ViewName,
		DestTable: op.DestTable,
		Query:     cleanQuery(op.Query),
	}
}

func replayModify(byName map[string]*expectedMV, database string, op ModifyQueryMaterializedViewOperation) {
	if op.Database != database {
		return
	}
	if mv, ok := byName[op.ViewName]; ok {
		mv.Query = cleanQuery(op.Query)
	}
}

func replayDrop(byName map[string]*expectedMV, database string, op DropTableOperation) {
	if op.Database != database {
		return
	}
	delete(byName, op.Table)
}

func cleanQuery(q string) string {
	return strings.TrimRight(strings.TrimSpace(q), "; \t\r\n")
}

var viewAliasPattern = regexp.MustCompile(`(?is)\bAS\s+SELECT\s+\*\s+FROM\s+(\S+?)\s*;?\s*$`)

// buildViewAliases maps every alias VIEW to the storage table it selects from,
// for both layouts. VIEWs that are not a bare `SELECT * FROM <table>`, or whose
// source is not a storage table, are not aliases and are left out.
func buildViewAliases(tables map[string]signozTable) map[string]string {
	aliases := map[string]string{}
	for key, t := range tables {
		if t.Engine != "View" {
			continue
		}
		m := viewAliasPattern.FindStringSubmatch(t.CreateTableQuery)
		if m == nil {
			continue
		}
		source := qualifyTableRef(t.Database, m[1])
		if src, ok := tables[source]; ok && isStorageEngine(src.Engine) {
			aliases[key] = source
		}
	}
	return aliases
}

// parseMVTarget returns the fully qualified TO table of an MV, or "" for an MV
// the repair must leave alone: one that owns its storage (ENGINE = ...) or a
// refreshable one. engine_full is useless here: ClickHouse leaves it empty for
// every TO-form MV.
func parseMVTarget(database, createTableQuery string) string {
	// Cut at the header/body separator so a TO inside the SELECT is ignored.
	head := createTableQuery
	if idx := strings.Index(head, " AS "); idx != -1 {
		head = head[:idx]
	}
	if strings.Contains(head, " REFRESH ") {
		return ""
	}
	// An inline engine may carry `TTL ... TO VOLUME`, which is not a target.
	if idx := strings.Index(head, " ENGINE"); idx != -1 {
		head = head[:idx]
	}
	idx := strings.Index(head, " TO ")
	if idx == -1 {
		return ""
	}
	target := strings.TrimSpace(head[idx+len(" TO "):])
	if cut := strings.IndexAny(target, " ("); cut != -1 {
		target = target[:cut]
	}
	return qualifyTableRef(database, target)
}

func qualifyTableRef(database, ref string) string {
	ref = strings.ReplaceAll(strings.TrimSpace(ref), "`", "")
	if ref == "" {
		return ""
	}
	if !strings.Contains(ref, ".") {
		return database + "." + ref
	}
	return ref
}

var (
	fromClausePattern  = regexp.MustCompile(`(?i)\bFROM\b`)
	selectStartPattern = regexp.MustCompile(`(?i)^\s*(SELECT|WITH)\b`)
)

// mvSelectBody returns the SELECT an MV runs. create_table_query is the full
// DDL and is preferred; as_select is only a fallback. A body without FROM is
// rejected: it still parses, but reads system.one, so recreating an MV from it
// would silently detach the MV from its source.
func mvSelectBody(createTableQuery, asSelect string) (string, bool) {
	if idx := strings.Index(createTableQuery, " AS "); idx != -1 {
		body := strings.TrimSpace(createTableQuery[idx+len(" AS "):])
		if isUsableSelect(body) {
			return body, true
		}
	}
	if isUsableSelect(asSelect) {
		return strings.TrimSpace(asSelect), true
	}
	return "", false
}

func isUsableSelect(s string) bool {
	return selectStartPattern.MatchString(s) && fromClausePattern.MatchString(s)
}

func isIdentByte(b byte) bool {
	return b == '_' || b == '$' || b == '`' ||
		(b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

// replaceTableRefs rewrites whole qualified table references in a single pass,
// so `db.usage_explorer` never matches inside `db.usage_explorer_mv` and a
// replacement is never rewritten again.
func replaceTableRefs(query string, replacements map[string]string) string {
	if len(replacements) == 0 {
		return query
	}
	keys := make([]string, 0, len(replacements))
	for k := range replacements {
		keys = append(keys, k)
	}
	// Longest first, so the most specific reference wins at a given position.
	sort.Slice(keys, func(i, j int) bool { return len(keys[i]) > len(keys[j]) })

	var out strings.Builder
	for i := 0; i < len(query); {
		matched := false
		if i == 0 || !(isIdentByte(query[i-1]) || query[i-1] == '.') {
			for _, k := range keys {
				end := i + len(k)
				if !strings.HasPrefix(query[i:], k) || (end < len(query) && isIdentByte(query[end])) {
					continue
				}
				out.WriteString(replacements[k])
				i = end
				matched = true
				break
			}
		}
		if !matched {
			out.WriteByte(query[i])
			i++
		}
	}
	return out.String()
}

type mvActionKind string

const (
	// Only the FROM clause is wrong: ALTER ... MODIFY QUERY changes it in place.
	mvActionModifyQuery mvActionKind = "modify-query"
	// The TO clause points at an alias VIEW: drop the MV and create it again.
	mvActionRebuild mvActionKind = "rebuild"
	// A migration that already ran should have left this MV behind.
	mvActionRecreate mvActionKind = "recreate-missing"
	// A fix is needed but cannot be applied safely.
	mvActionUnresolvable mvActionKind = "unresolvable"
)

type mvAction struct {
	Kind     mvActionKind
	Database string
	Name     string
	Target   string // new TO table, for rebuild and recreate
	Select   string // new body, for modify, rebuild and recreate
	Reason   string // why, for unresolvable
}

func (a mvAction) fqName() string { return a.Database + "." + a.Name }

func (a mvAction) String() string {
	switch a.Kind {
	case mvActionRebuild, mvActionRecreate:
		return fmt.Sprintf("%s %s (TO %s)", a.Kind, a.fqName(), a.Target)
	case mvActionUnresolvable:
		return fmt.Sprintf("%s %s: %s", a.Kind, a.fqName(), a.Reason)
	default:
		return fmt.Sprintf("%s %s", a.Kind, a.fqName())
	}
}

type mvPlan struct {
	Actions []mvAction
	// The catalog as it would look after every action succeeded.
	Simulated map[string]signozTable
}

func synthesizeMV(database, name, target, body string) signozTable {
	return signozTable{
		Database:         database,
		Name:             name,
		Engine:           "MaterializedView",
		CreateTableQuery: fmt.Sprintf("CREATE MATERIALIZED VIEW %s.%s TO %s AS %s", database, name, target, body),
		AsSelect:         body,
	}
}

// planStandaloneMVs decides every change and simulates its effect on the
// catalog. Only MVs that are provably wrong get an action — the target is an
// alias VIEW, the body reads one, or the MV is missing — and MVs that are not
// expected are never dropped.
func planStandaloneMVs(tables map[string]signozTable, expected []expectedMV) mvPlan {
	sim := make(map[string]signozTable, len(tables))
	for k, v := range tables {
		sim[k] = v
	}
	// MV changes never turn a VIEW into storage, so the aliases stay valid.
	aliases := buildViewAliases(tables)
	var actions []mvAction

	for _, key := range sortedKeys(tables) {
		mv := tables[key]
		if !mv.isMaterializedView() {
			continue
		}
		target := parseMVTarget(mv.Database, mv.CreateTableQuery)
		if target == "" {
			continue
		}
		newTarget := target
		if storage, ok := aliases[target]; ok {
			newTarget = storage
		}
		body, bodyOK := mvSelectBody(mv.CreateTableQuery, mv.AsSelect)
		newBody := body
		if bodyOK {
			newBody = replaceTableRefs(body, aliases)
		}

		toFix := newTarget != target
		fromFix := bodyOK && newBody != body
		action := mvAction{Database: mv.Database, Name: mv.Name, Target: newTarget, Select: newBody}
		switch {
		case !toFix && !fromFix:
			continue
		case !bodyOK:
			action.Kind = mvActionUnresolvable
			action.Reason = "its SELECT could not be read back"
		case toFix:
			action.Kind = mvActionRebuild
		default:
			action.Kind = mvActionModifyQuery
		}
		actions = append(actions, action)
		if action.Kind != mvActionUnresolvable {
			sim[key] = synthesizeMV(mv.Database, mv.Name, newTarget, newBody)
		}
	}

	for _, e := range expected {
		if _, ok := sim[e.fqName()]; ok {
			continue
		}
		target := qualifyTableRef(e.Database, e.DestTable)
		if storage, ok := aliases[target]; ok {
			target = storage
		}
		action := mvAction{
			Database: e.Database, Name: e.Name, Target: target, Select: replaceTableRefs(e.Query, aliases),
		}
		// Creating an MV over anything but storage would break INSERTs into its
		// source until the reaper removes it, on every retry of the migration.
		if dest, ok := tables[target]; !ok || !isStorageEngine(dest.Engine) {
			action.Kind = mvActionUnresolvable
			action.Reason = fmt.Sprintf("it is missing and its target %s is not a storage table", target)
			actions = append(actions, action)
			continue
		}
		action.Kind = mvActionRecreate
		actions = append(actions, action)
		sim[e.fqName()] = synthesizeMV(e.Database, e.Name, target, action.Select)
	}

	return mvPlan{Actions: actions, Simulated: sim}
}

// validateMaterializedViews returns every way the MVs are broken: an expected
// MV is missing, an MV writes to something that is not storage, or an MV reads
// an alias VIEW and therefore never fires.
func validateMaterializedViews(tables map[string]signozTable, expected []expectedMV) []string {
	aliases := buildViewAliases(tables)
	var problems []string

	for _, e := range expected {
		if _, ok := tables[e.fqName()]; !ok {
			problems = append(problems, fmt.Sprintf("%s is missing", e.fqName()))
		}
	}

	for _, key := range sortedKeys(tables) {
		mv := tables[key]
		if !mv.isMaterializedView() {
			continue
		}
		target := parseMVTarget(mv.Database, mv.CreateTableQuery)
		if target == "" {
			continue
		}
		if dest, ok := tables[target]; !ok {
			problems = append(problems, fmt.Sprintf("%s writes to %s, which does not exist", key, target))
		} else if !isStorageEngine(dest.Engine) {
			problems = append(problems, fmt.Sprintf("%s writes to %s (%s), not a storage table", key, target, dest.Engine))
		}
		if body, ok := mvSelectBody(mv.CreateTableQuery, mv.AsSelect); !ok {
			problems = append(problems, fmt.Sprintf("%s: its SELECT could not be read back", key))
		} else if replaceTableRefs(body, aliases) != body {
			problems = append(problems, fmt.Sprintf("%s reads from an alias VIEW, so it never fires", key))
		}
	}
	return problems
}

func sortedKeys(tables map[string]signozTable) []string {
	keys := make([]string, 0, len(tables))
	for k := range tables {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (m *MigrationManager) loadSignozTables(ctx context.Context) (map[string]signozTable, error) {
	databases := make([]string, len(Databases))
	for i, db := range Databases {
		databases[i] = "'" + db + "'"
	}
	rows, err := m.conn.Query(ctx, fmt.Sprintf(`
		SELECT database, name, engine, create_table_query, as_select
		FROM system.tables
		WHERE database IN (%s) AND is_temporary = 0
	`, strings.Join(databases, ", ")))
	if err != nil {
		return nil, fmt.Errorf("failed to list signoz tables: %w", err)
	}
	defer rows.Close()

	tables := map[string]signozTable{}
	for rows.Next() {
		var t signozTable
		if err := rows.Scan(&t.Database, &t.Name, &t.Engine, &t.CreateTableQuery, &t.AsSelect); err != nil {
			return nil, fmt.Errorf("failed to scan signoz table: %w", err)
		}
		tables[t.fqName()] = t
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to list signoz tables: %w", err)
	}
	return tables, nil
}

// MVCheckReport describes what RecreateMaterializedViewsForStandalone would do
// and what would still be wrong afterwards, without changing anything.
type MVCheckReport struct {
	Actions  []string
	Problems []string
}

// CheckMaterializedViewsForStandalone is the read-only preflight for
// RecreateMaterializedViewsForStandalone: the same plan, nothing executed. Any
// reported problem would make sync fail. It assumes no migration is pending;
// sync runs pending migrations before the repair.
func (m *MigrationManager) CheckMaterializedViewsForStandalone(ctx context.Context) (MVCheckReport, error) {
	tables, err := m.loadSignozTables(ctx)
	if err != nil {
		return MVCheckReport{}, err
	}
	expected := m.expectedMaterializedViews()
	plan := planStandaloneMVs(tables, expected)

	report := MVCheckReport{Problems: validateMaterializedViews(plan.Simulated, expected)}
	for _, a := range plan.Actions {
		report.Actions = append(report.Actions, a.String())
	}
	return report, nil
}

// RecreateMaterializedViewsForStandalone makes every MV in the signoz databases
// read and write storage tables, recreates expected MVs that are missing, and
// fails if anything is still wrong afterwards. It runs on every sync and async,
// so an MV that is already correct must come out untouched.
func (m *MigrationManager) RecreateMaterializedViewsForStandalone(ctx context.Context) error {
	if m.clusterName != "" {
		return nil // Only for standalone mode
	}

	m.logger.Info("Checking materialized views for standalone mode")

	tables, err := m.loadSignozTables(ctx)
	if err != nil {
		return err
	}
	expected := m.expectedMaterializedViews()
	plan := planStandaloneMVs(tables, expected)

	var repairErrs []error
	for _, action := range plan.Actions {
		if err := m.executeMVAction(ctx, action); err != nil {
			m.logger.Error("Materialized view repair step failed", zap.String("action", action.String()), zap.Error(err))
			repairErrs = append(repairErrs, err)
		}
	}

	// The gate: re-read the catalog and refuse to finish while anything is
	// wrong. DDL just run on one node may not be visible on another yet, so
	// when something changed, give it a few reads before deciding.
	attempts := 1
	if len(plan.Actions) > 0 {
		attempts = mvGateAttempts
	}
	var problems []string
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(m.mvGateRetryDelay):
			}
		}
		if tables, err = m.loadSignozTables(ctx); err != nil {
			return err
		}
		if problems = validateMaterializedViews(tables, expected); len(problems) == 0 {
			break
		}
	}

	if len(problems) > 0 {
		for _, p := range problems {
			m.logger.Error("Materialized view is misconfigured", zap.String("problem", p))
		}
		gateErr := errors.Join(append([]error{fmt.Errorf(
			"%d materialized view problem(s) remain in standalone mode: %s",
			len(problems), strings.Join(problems, "; "))}, repairErrs...)...)
		if m.mvGateWarnOnly {
			m.logger.Error("Materialized view gate is in warn-only mode; continuing despite problems", zap.Error(gateErr))
			return nil
		}
		return gateErr
	}

	for _, err := range repairErrs {
		// The catalog validated anyway, so the failed step did not matter.
		m.logger.Warn("Materialized view repair step failed but the result validated", zap.Error(err))
	}
	m.logger.Info("Materialized views are consistent for standalone mode",
		zap.Int("actions", len(plan.Actions)))
	return nil
}

func (m *MigrationManager) executeMVAction(ctx context.Context, a mvAction) error {
	fq := a.fqName()
	m.logger.Info("Materialized view repair", zap.String("action", a.String()))

	switch a.Kind {
	case mvActionModifyQuery:
		return m.execMV(ctx, ModifyQueryMaterializedViewOperation{
			Database: a.Database, ViewName: a.Name, Query: a.Select,
		}.ToSQL())
	case mvActionRebuild:
		// A rebuild is only planned when the MV writes to an alias VIEW: it
		// already fails every INSERT into its source and ObsessionDB's reaper
		// is about to drop it, so there is no working MV to protect. Staging a
		// replacement under another name would leave a stale registration in
		// the shared metadata backend (it is keyed by name).
		if err := m.execMV(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s", fq)); err != nil {
			return err
		}
		return m.createMV(ctx, a)
	case mvActionRecreate:
		return m.createMV(ctx, a)
	case mvActionUnresolvable:
		return fmt.Errorf("%s cannot be repaired: %s", fq, a.Reason)
	}
	return fmt.Errorf("unknown materialized view action %q for %s", a.Kind, fq)
}

func (m *MigrationManager) createMV(ctx context.Context, a mvAction) error {
	// IF NOT EXISTS: a concurrent migrator may have created it in between.
	return m.execMV(ctx, fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS %s", a.fqName(), a.Target, a.Select))
}

func (m *MigrationManager) execMV(ctx context.Context, sql string) error {
	m.logger.Info("Running materialized view statement", zap.String("sql", sql))
	if err := m.conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("%s: %w", sql, err)
	}
	return nil
}
