package schemamigrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/SigNoz/signoz-otel-collector/constants"
	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
)

var (
	ErrFailedToGetConn                  = errors.New("failed to get conn")
	ErrFailedToGetHostAddrs             = errors.New("failed to get host addrs")
	ErrFailedToCreateDBs                = errors.New("failed to create dbs")
	ErrFailedToRunOperation             = errors.New("failed to run operation")
	ErrFailedToWaitForMutations         = errors.New("failed to wait for mutations")
	ErrFailedToWaitForDDLQueue          = errors.New("failed to wait for DDL queue")
	ErrFailedToWaitForDistributionQueue = errors.New("failed to wait for distribution queue")
	ErrFailedToRunSquashedMigrations    = errors.New("failed to run squashed migrations")
	ErrFailedToCreateSchemaMigrationsV2 = errors.New("failed to create schema_migrations_v2 table")
	ErrDistributionQueueError           = errors.New("distribution_queue has entries with error_count != 0 or is_blocked = 1")

	legacyMigrationsTable = "schema_migrations"
	SignozLogsDB          = "signoz_logs"
	SignozMetricsDB       = "signoz_metrics"
	SignozTracesDB        = "signoz_traces"
	SignozMetadataDB      = "signoz_metadata"
	SignozAnalyticsDB     = "signoz_analytics"
	SignozMeterDB         = "signoz_meter"
	Databases             = []string{SignozTracesDB, SignozMetricsDB, SignozLogsDB, SignozMetadataDB, SignozAnalyticsDB, SignozMeterDB}

	InProgressStatus = "in-progress"
	FinishedStatus   = "finished"
	FailedStatus     = "failed"
)

type Mutation struct {
	Database         string    `ch:"database"`
	Table            string    `ch:"table"`
	MutationID       string    `ch:"mutation_id"`
	Command          string    `ch:"command"`
	CreateTime       time.Time `ch:"create_time"`
	PartsToDo        int64     `ch:"parts_to_do"`
	LatestFailReason string    `ch:"latest_fail_reason"`
}

type DistributedDDLQueue struct {
	Entry           string    `ch:"entry"`
	Cluster         string    `ch:"cluster"`
	Query           string    `ch:"query"`
	QueryCreateTime time.Time `ch:"query_create_time"`
	Host            string    `ch:"host"`
	Port            uint16    `ch:"port"`
	Status          string    `ch:"status"`
	ExceptionCode   string    `ch:"exception_code"`
}

type SchemaMigrationRecord struct {
	MigrationID uint64
	UpItems     []Operation
	DownItems   []Operation
}

// BaseTableInfo stores information about base tables for standalone mode
type BaseTableInfo struct {
	Database    string
	Table       string
	Engine      TableEngine
	Columns     []Column
	Indexes     []Index
	Projections []Projection
}

// MigrationManager is the manager for the schema migrations.
type MigrationManager struct {
	// addrs is the list of addresses of the hosts in the cluster.
	addrs    []string
	addrsMux sync.Mutex
	conn     clickhouse.Conn
	connOpts clickhouse.Options
	conns    map[string]clickhouse.Conn

	clusterName        string
	replicationEnabled bool
	logger             *zap.Logger
	backoff            *backoff.ExponentialBackOff
	development        bool

	// Standalone mode tracking: base table name -> distributed table name
	// e.g., "signoz_logs.logs" -> "signoz_logs.distributed_logs"
	baseToDistributed map[string]string
	// Standalone mode tracking: base table name -> engine info
	baseTableEngines map[string]BaseTableInfo
	// Standalone mode tracking: tables actually converted to VIEWs
	// Only these tables should have ALTERs redirected
	convertedToViews map[string]bool
	// Standalone mode tracking: base tables that should be created as VIEWs
	// (because the distributed_ table is the real storage)
	pendingBaseTableViews map[string]string // base table name -> distributed table name
}

type Option func(*MigrationManager)

// NewMigrationManager creates a new migration manager.
func NewMigrationManager(opts ...Option) (*MigrationManager, error) {
	mgr := &MigrationManager{
		logger: zap.NewNop(),
		// the default backoff is good enough for our use case
		// no mutation should be running for more than 15 minutes, if it is, we should fail fast
		backoff:               backoff.NewExponentialBackOff(),
		replicationEnabled:    false,
		conns:                 make(map[string]clickhouse.Conn),
		baseToDistributed:     make(map[string]string),
		baseTableEngines:      make(map[string]BaseTableInfo),
		convertedToViews:      make(map[string]bool),
		pendingBaseTableViews: make(map[string]string),
	}
	for _, opt := range opts {
		opt(mgr)
	}
	if mgr.conn == nil {
		return nil, errors.New("conn is required")
	}
	return mgr, nil
}

func WithClusterName(clusterName string) Option {
	return func(mgr *MigrationManager) {
		mgr.clusterName = clusterName
	}
}

func WithDevelopment(development bool) Option {
	return func(mgr *MigrationManager) {
		mgr.development = development
	}
}

func WithReplicationEnabled(replicationEnabled bool) Option {
	return func(mgr *MigrationManager) {
		mgr.replicationEnabled = replicationEnabled
	}
}

func WithConn(conn clickhouse.Conn) Option {
	return func(mgr *MigrationManager) {
		mgr.conn = conn
	}
}

func WithConnOptions(opts clickhouse.Options) Option {
	return func(mgr *MigrationManager) {
		mgr.connOpts = opts
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(mgr *MigrationManager) {
		mgr.logger = logger
	}
}

func WithBackoff(backoff *backoff.ExponentialBackOff) Option {
	return func(mgr *MigrationManager) {
		mgr.backoff = backoff
	}
}

func (m *MigrationManager) createDBs() error {
	for _, db := range Databases {
		var cmd string
		if m.clusterName != "" {
			cmd = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s ON CLUSTER %s", db, m.clusterName)
		} else {
			// Standalone mode: no ON CLUSTER
			cmd = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db)
		}
		if err := m.conn.Exec(context.Background(), cmd); err != nil {
			return errors.Join(ErrFailedToCreateDBs, err)
		}
	}
	m.logger.Info("Created databases", zap.Strings("dbs", Databases))
	return nil
}

// Bootstrap migrates the schema up for the migrations tables
func (m *MigrationManager) Bootstrap() error {
	if err := m.createDBs(); err != nil {
		return errors.Join(ErrFailedToCreateDBs, err)
	}
	m.logger.Info("Creating schema migrations tables")
	for _, migration := range V2MigrationTablesLogs {
		for _, item := range migration.UpItems {
			if err := m.RunOperation(context.Background(), item, migration.MigrationID, SignozLogsDB, true); err != nil {
				return errors.Join(ErrFailedToCreateSchemaMigrationsV2, err)
			}
		}
	}

	for _, migration := range V2MigrationTablesMetrics {
		for _, item := range migration.UpItems {
			if err := m.RunOperation(context.Background(), item, migration.MigrationID, SignozMetricsDB, true); err != nil {
				return errors.Join(ErrFailedToCreateSchemaMigrationsV2, err)
			}
		}
	}

	for _, migration := range V2MigrationTablesTraces {
		for _, item := range migration.UpItems {
			if err := m.RunOperation(context.Background(), item, migration.MigrationID, SignozTracesDB, true); err != nil {
				return errors.Join(ErrFailedToCreateSchemaMigrationsV2, err)
			}
		}
	}

	for _, migration := range V2MigrationTablesMetadata {
		for _, item := range migration.UpItems {
			if err := m.RunOperation(context.Background(), item, migration.MigrationID, SignozMetadataDB, true); err != nil {
				return errors.Join(ErrFailedToCreateSchemaMigrationsV2, err)
			}
		}
	}

	for _, migration := range V2MigrationTablesAnalytics {
		for _, item := range migration.UpItems {
			if err := m.RunOperation(context.Background(), item, migration.MigrationID, SignozAnalyticsDB, true); err != nil {
				return errors.Join(ErrFailedToCreateSchemaMigrationsV2, err)
			}
		}
	}

	for _, migration := range V2MigrationTablesMeter {
		for _, item := range migration.UpItems {
			if err := m.RunOperation(context.Background(), item, migration.MigrationID, SignozMeterDB, true); err != nil {
				return errors.Join(ErrFailedToCreateSchemaMigrationsV2, err)
			}
		}
	}

	m.logger.Info("Created schema migrations tables")
	return nil
}

// shouldRunSquashed returns true if the legacy migrations table exists and the v2 table does not exist
func (m *MigrationManager) shouldRunSquashed(ctx context.Context, db string) (bool, error) {
	var count uint64
	var err error
	if m.clusterName != "" {
		err = m.conn.QueryRow(ctx, "SELECT count(*) FROM clusterAllReplicas($1, system.tables) WHERE database = $2 AND name = $3", m.clusterName, db, legacyMigrationsTable).Scan(&count)
	} else {
		// Standalone mode: use local system.tables
		err = m.conn.QueryRow(ctx, "SELECT count(*) FROM system.tables WHERE database = $1 AND name = $2", db, legacyMigrationsTable).Scan(&count)
	}
	if err != nil {
		return false, err
	}
	var countV2 uint64
	if m.clusterName != "" {
		err = m.conn.QueryRow(ctx, "SELECT count(*) FROM clusterAllReplicas($1, system.tables) WHERE database = $2 AND name = 'schema_migrations_v2'", m.clusterName, db).Scan(&countV2)
	} else {
		// Standalone mode: use local system.tables
		err = m.conn.QueryRow(ctx, "SELECT count(*) FROM system.tables WHERE database = $1 AND name = 'schema_migrations_v2'", db).Scan(&countV2)
	}
	if err != nil {
		return false, err
	}
	didMigrateV2 := false
	if countV2 > 0 {
		// if there are non-zero v2 migrations, we don't need to run the squashed migrations
		// fetch count of v2 migrations that are not finished
		var countMigrations uint64
		// In standalone mode, use local table instead of distributed table
		migrationsTable := "distributed_schema_migrations_v2"
		if m.clusterName == "" {
			migrationsTable = "schema_migrations_v2"
		}
		err = m.conn.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s", db, migrationsTable)).Scan(&countMigrations)
		if err != nil {
			return false, err
		}
		didMigrateV2 = countMigrations > 0
	}
	// we want to run the squashed migrations only if the legacy migrations do not exist
	// and v2 migrations did not run
	return count == 0 && !didMigrateV2, nil
}

func (m *MigrationManager) runCustomRetentionMigrationsForLogs(ctx context.Context) error {
	m.logger.Info("Checking if should run squashed migrations for logs")
	should, err := m.shouldRunSquashed(ctx, "signoz_logs")
	if err != nil {
		return err
	}
	// if the legacy migrations table exists, we don't need to run the custom retention migrations
	if !should {
		m.logger.Info("skipping custom retention migrations")
		return nil
	}
	m.logger.Info("Running custom retention migrations for logs")
	for _, migration := range CustomRetentionLogsMigrations {
		for _, item := range migration.UpItems {
			if err := m.RunOperation(ctx, item, migration.MigrationID, "signoz_logs", false); err != nil {
				return err
			}
		}
	}
	m.logger.Info("Custom retention migrations for logs completed")
	return nil
}

//nolint:unused
func (m *MigrationManager) runSquashedMigrationsForLogs(ctx context.Context) error {
	m.logger.Info("Checking if should run squashed migrations for logs")
	should, err := m.shouldRunSquashed(ctx, "signoz_logs")
	if err != nil {
		return err
	}
	// if the legacy migrations table exists, we don't need to run the squashed migrations
	if !should {
		m.logger.Info("skipping squashed migrations")
		return nil
	}
	m.logger.Info("Running squashed migrations for logs")
	for _, migration := range SquashedLogsMigrations {
		for _, item := range migration.UpItems {
			if err := m.RunOperation(ctx, item, migration.MigrationID, "signoz_logs", false); err != nil {
				return err
			}
		}
	}
	m.logger.Info("Squashed migrations for logs completed")
	return nil
}

func (m *MigrationManager) runSquashedMigrationsForMetrics(ctx context.Context) error {
	m.logger.Info("Checking if should run squashed migrations for metrics")
	should, err := m.shouldRunSquashed(ctx, SignozMetricsDB)
	if err != nil {
		return err
	}
	if !should {
		m.logger.Info("skipping squashed migrations")
		return nil
	}
	m.logger.Info("Running squashed migrations for metrics")
	for _, migration := range SquashedMetricsMigrations {
		for _, item := range migration.UpItems {
			if err := m.RunOperation(ctx, item, migration.MigrationID, SignozMetricsDB, false); err != nil {
				return err
			}
		}
	}
	m.logger.Info("Squashed migrations for metrics completed")
	return nil
}

func (m *MigrationManager) runSquashedMigrationsForTraces(ctx context.Context) error {
	m.logger.Info("Checking if should run squashed migrations for traces")
	should, err := m.shouldRunSquashed(ctx, SignozTracesDB)
	if err != nil {
		return err
	}
	if !should {
		m.logger.Info("skipping squashed migrations")
		return nil
	}
	m.logger.Info("Running squashed migrations for traces")
	for _, migration := range SquashedTracesMigrations {
		for _, item := range migration.UpItems {
			if err := m.RunOperation(ctx, item, migration.MigrationID, SignozTracesDB, false); err != nil {
				return err
			}
		}
	}
	m.logger.Info("Squashed migrations for traces completed")
	return nil
}

func (m *MigrationManager) RunSquashedMigrations(ctx context.Context) error {
	m.logger.Info("Running squashed migrations")

	// In standalone mode, first scan for existing VIEWs to populate the baseToDistributed map.
	// This is needed because on restarts, squashed migrations may be skipped, so we need to
	// discover which tables are already VIEWs to skip ALTER TABLE operations on them.
	if m.clusterName == "" {
		if err := m.populateBaseToDistributedFromExistingViews(context.Background()); err != nil {
			m.logger.Warn("Failed to populate baseToDistributed from existing VIEWs", zap.Error(err))
			// Continue anyway - not fatal
		}
	}

	if err := m.runCustomRetentionMigrationsForLogs(ctx); err != nil {
		return errors.Join(ErrFailedToRunSquashedMigrations, err)
	}
	if err := m.runSquashedMigrationsForMetrics(ctx); err != nil {
		return errors.Join(ErrFailedToRunSquashedMigrations, err)
	}
	if err := m.runSquashedMigrationsForTraces(ctx); err != nil {
		return errors.Join(ErrFailedToRunSquashedMigrations, err)
	}

	// In standalone mode, distributed_* tables are already created as VIEWs pointing
	// to base tables during table creation. No additional conversion needed.
	// - Base tables: SharedMergeTree (actual storage, OTEL writes here)
	// - Distributed tables: VIEWs (aliases for query compatibility)
	// - MVs: Write to base tables (original behavior, no change needed)

	m.logger.Info("Squashed migrations completed")
	return nil
}

// populateBaseToDistributedFromExistingViews recovers the baseToDistributed and
// convertedToViews maps from the actual database state. On restarts, squashed
// migrations are skipped so these maps would otherwise be empty.
//
// Finds VIEWs like "time_series_v4 AS SELECT * FROM distributed_time_series_v4"
// and records that time_series_v4 is a VIEW that needs ALTER redirects.
func (m *MigrationManager) populateBaseToDistributedFromExistingViews(ctx context.Context) error {
	if m.clusterName != "" {
		return nil // Only for standalone mode
	}

	m.logger.Info("Scanning for base table VIEWs pointing to distributed tables")

	// Find base table VIEWs (non-distributed_*) that point to distributed_* tables.
	query := `
		SELECT database, name, as_select
		FROM system.tables
		WHERE engine = 'View'
		  AND database LIKE 'signoz_%'
		  AND name NOT LIKE 'distributed_%'
	`

	rows, err := m.conn.Query(ctx, query)
	if err != nil {
		m.logger.Error("Failed to query views", zap.Error(err))
		return err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var database, name, asSelect string
		if err := rows.Scan(&database, &name, &asSelect); err != nil {
			m.logger.Error("Failed to scan view row", zap.Error(err))
			continue
		}

		// Extract the target table from the SELECT statement
		// The as_select is like: "SELECT * FROM signoz_metrics.distributed_time_series_v4"
		asSelectLower := strings.ToLower(asSelect)
		fromIdx := strings.Index(asSelectLower, " from ")
		if fromIdx == -1 {
			continue
		}
		afterFrom := strings.TrimSpace(asSelect[fromIdx+6:])
		parts := strings.Fields(afterFrom)
		if len(parts) == 0 {
			continue
		}
		targetTableRef := parts[0]

		// Verify the target is a distributed_* table
		targetParts := strings.SplitN(targetTableRef, ".", 2)
		targetTable := targetTableRef
		if len(targetParts) == 2 {
			targetTable = targetParts[1]
		}
		if !strings.HasPrefix(targetTable, "distributed_") {
			// Not pointing to a distributed_* table, skip
			continue
		}

		// The VIEW is the base table, the SELECT target is the distributed table
		baseTableKey := database + "." + name
		m.baseToDistributed[baseTableKey] = targetTableRef
		m.convertedToViews[baseTableKey] = true
		m.logger.Info("Found base table VIEW pointing to distributed table",
			zap.String("base_view", baseTableKey),
			zap.String("distributed_table", targetTableRef))
		count++
	}

	m.logger.Info("Finished scanning base table VIEWs", zap.Int("count", count))
	return nil
}

// ConvertBaseTablesToViews converts base tables to VIEWs in standalone mode
// This is needed because:
// - otel-collector writes to distributed_* tables
// - MVs read from base tables (e.g., logs)
// - We want MVs to see data from distributed_* tables
// So we: DROP base table, CREATE VIEW base AS SELECT * FROM distributed_*
func (m *MigrationManager) ConvertBaseTablesToViews(ctx context.Context) error {
	if m.clusterName != "" {
		return nil // Only for standalone mode
	}

	// First, find all tables that are targets of materialized views.
	// These tables CANNOT be converted to VIEWs because MVs need to write to them.
	mvTargets := make(map[string]bool)
	mvQuery := `
		SELECT database, name, engine_full
		FROM system.tables
		WHERE engine = 'MaterializedView'
		  AND database LIKE 'signoz_%'
	`
	mvRows, err := m.conn.Query(ctx, mvQuery)
	if err != nil {
		m.logger.Warn("Failed to query materialized views for target detection", zap.Error(err))
		// Continue anyway - we'll skip tables conservatively
	} else {
		defer mvRows.Close()
		for mvRows.Next() {
			var database, name, engineFull string
			if err := mvRows.Scan(&database, &name, &engineFull); err != nil {
				continue
			}
			// engine_full contains the target table, e.g., "MaterializedView TO signoz_traces.dependency_graph_minutes_v2"
			if strings.Contains(engineFull, " TO ") {
				parts := strings.Split(engineFull, " TO ")
				if len(parts) > 1 {
					targetRef := strings.TrimSpace(parts[1])
					// May contain additional clauses after the table name
					targetParts := strings.Fields(targetRef)
					if len(targetParts) > 0 {
						targetTable := targetParts[0]
						mvTargets[targetTable] = true
						m.logger.Info("Found MV target table (will not convert to VIEW)",
							zap.String("target", targetTable),
							zap.String("mv", database+"."+name))
					}
				}
			}
		}
	}

	m.logger.Info("Converting base tables to VIEWs",
		zap.Int("count", len(m.baseToDistributed)),
		zap.Int("mv_targets", len(mvTargets)))

	for baseTable, distTable := range m.baseToDistributed {
		parts := strings.Split(baseTable, ".")
		if len(parts) != 2 {
			m.logger.Warn("Invalid base table name", zap.String("table", baseTable))
			continue
		}
		database := parts[0]
		tableName := parts[1]

		// Skip internal schema migration tables - these must remain as actual tables
		// because they're used to track migration state, not data tables
		if strings.Contains(tableName, "schema_migrations") {
			m.logger.Info("Skipping internal schema_migrations table",
				zap.String("table", baseTable))
			continue
		}

		// Skip tables that are targets of materialized views - MVs need to write to them
		// Note: RecreateMaterializedViewsForStandalone runs BEFORE this function,
		// so MVs now target distributed_* tables, not base tables.
		if mvTargets[baseTable] {
			m.logger.Info("Skipping MV target table (MVs write here)",
				zap.String("table", baseTable))
			continue
		}

		distParts := strings.Split(distTable, ".")
		if len(distParts) != 2 {
			m.logger.Warn("Invalid distributed table name", zap.String("table", distTable))
			continue
		}
		distTableName := distParts[1]

		m.logger.Info("Converting base table to VIEW",
			zap.String("base", baseTable),
			zap.String("distributed", distTable))

		// DROP the base table
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", database, tableName)
		m.logger.Info("Dropping base table", zap.String("sql", dropSQL))
		if err := m.conn.Exec(ctx, dropSQL); err != nil {
			m.logger.Error("Failed to drop base table", zap.Error(err), zap.String("table", baseTable))
			return err
		}

		// CREATE VIEW pointing to distributed table
		viewSQL := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s",
			database, tableName, database, distTableName)
		m.logger.Info("Creating VIEW", zap.String("sql", viewSQL))
		if err := m.conn.Exec(ctx, viewSQL); err != nil {
			m.logger.Error("Failed to create VIEW", zap.Error(err), zap.String("table", baseTable))
			return err
		}

		// Track that this table was actually converted to a VIEW
		m.convertedToViews[baseTable] = true
	}

	m.logger.Info("Finished converting base tables to VIEWs")
	return nil
}

// RecreateMaterializedViewsForStandalone recreates MVs to write to distributed_* tables.
// In standalone mode, base tables are converted to VIEWs, which can't receive INSERTs.
// So MVs need to write directly to distributed_* tables instead.
func (m *MigrationManager) RecreateMaterializedViewsForStandalone(ctx context.Context) error {
	if m.clusterName != "" {
		return nil // Only for standalone mode
	}

	m.logger.Info("Recreating Materialized Views for standalone mode")

	// Query all materialized views that write to base tables (not distributed_*)
	query := `
		SELECT database, name, as_select, engine_full
		FROM system.tables
		WHERE engine = 'MaterializedView'
		  AND database LIKE 'signoz_%'
		  AND engine_full NOT LIKE '%distributed_%'
	`

	rows, err := m.conn.Query(ctx, query)
	if err != nil {
		m.logger.Error("Failed to query materialized views", zap.Error(err))
		return err
	}
	defer rows.Close()

	type mvInfo struct {
		Database   string
		Name       string
		AsSelect   string
		EngineFull string
	}

	var mvsToRecreate []mvInfo
	for rows.Next() {
		var mv mvInfo
		if err := rows.Scan(&mv.Database, &mv.Name, &mv.AsSelect, &mv.EngineFull); err != nil {
			m.logger.Error("Failed to scan MV row", zap.Error(err))
			return err
		}
		mvsToRecreate = append(mvsToRecreate, mv)
	}

	m.logger.Info("Found MVs to recreate", zap.Int("count", len(mvsToRecreate)))

	// Fallback mapping for MVs when engine_full is empty (ClickHouse Cloud)
	mvToDestTable := map[string]string{
		// Traces MVs
		"root_operations":                                "signoz_traces.top_level_operations",
		"sub_root_operations":                            "signoz_traces.top_level_operations",
		"usage_explorer_mv":                              "signoz_traces.usage_explorer",
		"dependency_graph_minutes_db_calls_mv_v2":        "signoz_traces.dependency_graph_minutes_v2",
		"dependency_graph_minutes_messaging_calls_mv_v2": "signoz_traces.dependency_graph_minutes_v2",
		"dependency_graph_minutes_service_calls_mv_v2":   "signoz_traces.dependency_graph_minutes_v2",
		"durationSortMV":                                 "signoz_traces.durationSort",
		"trace_summary_mv":                               "signoz_traces.trace_summary",
		// Metrics MVs
		"samples_v4_agg_5m_mv":                  "signoz_metrics.samples_v4_agg_5m",
		"samples_v4_agg_30m_mv":                 "signoz_metrics.samples_v4_agg_30m",
		"time_series_v4_6hrs_mv":                "signoz_metrics.time_series_v4_6hrs",
		"time_series_v4_1day_mv":                "signoz_metrics.time_series_v4_1day",
		"time_series_v4_1week_mv":               "signoz_metrics.time_series_v4_1week",
		"time_series_v4_6hrs_mv_separate_attrs": "signoz_metrics.time_series_v4_6hrs",
		"time_series_v4_1day_mv_separate_attrs": "signoz_metrics.time_series_v4_1day",
		"time_series_v4_1week_mv_separate_attrs": "signoz_metrics.time_series_v4_1week",
		// Logs MVs
		"attribute_keys_string_final_mv":  "signoz_logs.logs_attribute_keys",
		"attribute_keys_int64_final_mv":   "signoz_logs.logs_attribute_keys",
		"attribute_keys_float64_final_mv": "signoz_logs.logs_attribute_keys",
		"attribute_keys_bool_final_mv":    "signoz_logs.logs_attribute_keys",
		"resource_keys_string_final_mv":   "signoz_logs.logs_resource_keys",
	}

	for _, mv := range mvsToRecreate {
		// Parse the destination table from engine_full (e.g., "MaterializedView(...) TO signoz_metrics.time_series_v4_6hrs")
		// The engine_full contains the destination table info
		destTable := ""
		if idx := strings.Index(mv.EngineFull, " TO "); idx != -1 {
			destTable = strings.TrimSpace(mv.EngineFull[idx+4:])
			// Remove any trailing parts after the table name
			if spaceIdx := strings.Index(destTable, " "); spaceIdx != -1 {
				destTable = destTable[:spaceIdx]
			}
		}

		// Fallback: use known mapping if engine_full is empty (ClickHouse Cloud)
		if destTable == "" {
			if dest, ok := mvToDestTable[mv.Name]; ok {
				destTable = dest
				m.logger.Info("Using fallback destination table for MV",
					zap.String("mv", mv.Name), zap.String("dest", destTable))
			}
		}

		if destTable == "" {
			m.logger.Warn("Could not parse destination table from MV", zap.String("mv", mv.Name), zap.String("engine_full", mv.EngineFull))
			continue
		}

		// Extract database and table name from destTable
		parts := strings.Split(destTable, ".")
		if len(parts) != 2 {
			m.logger.Warn("Invalid destination table format", zap.String("dest", destTable))
			continue
		}
		destDB := parts[0]
		destTableName := parts[1]

		// Skip if already pointing to distributed_*
		if strings.HasPrefix(destTableName, "distributed_") {
			continue
		}

		// Build the new destination table name
		newDestTable := fmt.Sprintf("%s.distributed_%s", destDB, destTableName)

		// Check if the distributed table exists
		existsQuery := fmt.Sprintf("EXISTS %s", newDestTable)
		var exists uint8
		if err := m.conn.QueryRow(ctx, existsQuery).Scan(&exists); err != nil || exists != 1 {
			m.logger.Warn("Distributed table does not exist, skipping MV recreation",
				zap.String("mv", mv.Name), zap.String("distributed_table", newDestTable))
			continue
		}

		m.logger.Info("Recreating MV to write to distributed table",
			zap.String("mv", mv.Database+"."+mv.Name),
			zap.String("old_dest", destTable),
			zap.String("new_dest", newDestTable))

		// Drop the old MV
		dropSQL := fmt.Sprintf("DROP VIEW IF EXISTS %s.%s", mv.Database, mv.Name)
		m.logger.Info("Dropping old MV", zap.String("sql", dropSQL))
		if err := m.conn.Exec(ctx, dropSQL); err != nil {
			m.logger.Error("Failed to drop MV", zap.Error(err), zap.String("mv", mv.Name))
			return err
		}

		// Recreate the MV with the new destination
		createSQL := fmt.Sprintf("CREATE MATERIALIZED VIEW %s.%s TO %s AS %s",
			mv.Database, mv.Name, newDestTable, mv.AsSelect)
		m.logger.Info("Creating new MV", zap.String("sql", createSQL))
		if err := m.conn.Exec(ctx, createSQL); err != nil {
			m.logger.Error("Failed to create MV", zap.Error(err), zap.String("mv", mv.Name))
			return err
		}
	}

	m.logger.Info("Finished recreating Materialized Views for standalone mode")
	return nil
}

// HostAddrs returns the addresses of the all hosts in the cluster.
func (m *MigrationManager) HostAddrs() ([]string, error) {
	if m.development || m.clusterName == "" {
		// In standalone mode (no cluster), return nil
		return nil, nil
	}
	m.addrsMux.Lock()
	defer m.addrsMux.Unlock()
	if len(m.addrs) != 0 {
		return m.addrs, nil
	}

	hostAddrs := make(map[string]struct{})
	query := "SELECT DISTINCT host_address, port FROM system.clusters WHERE host_address NOT IN ['localhost', '127.0.0.1', '::1'] AND cluster = $1"
	rows, err := m.conn.Query(context.Background(), query, m.clusterName)
	if err != nil {
		return nil, errors.Join(ErrFailedToGetHostAddrs, err)
	}
	defer rows.Close()
	for rows.Next() {
		var hostAddr string
		var port uint16
		if err := rows.Scan(&hostAddr, &port); err != nil {
			return nil, errors.Join(ErrFailedToGetHostAddrs, err)
		}

		addr, err := netip.ParseAddr(hostAddr)
		if err != nil {
			return nil, errors.Join(ErrFailedToGetHostAddrs, err)
		}

		addrPort := netip.AddrPortFrom(addr, port)
		hostAddrs[addrPort.String()] = struct{}{}
	}

	if len(hostAddrs) != 0 {
		// connect to other host and do the same thing
		for hostAddr := range hostAddrs {
			m.logger.Info("Connecting to new host", zap.String("host", hostAddr))
			opts := m.connOpts
			opts.Addr = []string{hostAddr}
			conn, err := clickhouse.Open(&opts)
			if err != nil {
				return nil, errors.Join(ErrFailedToGetConn, err)
			}
			rows, err := conn.Query(context.Background(), query, m.clusterName)
			if err != nil {
				return nil, errors.Join(ErrFailedToGetConn, err)
			}
			defer rows.Close()
			for rows.Next() {
				var hostAddr string
				var port uint16
				if err := rows.Scan(&hostAddr, &port); err != nil {
					return nil, errors.Join(ErrFailedToGetHostAddrs, err)
				}

				addr, err := netip.ParseAddr(hostAddr)
				if err != nil {
					return nil, errors.Join(ErrFailedToGetHostAddrs, err)
				}

				addrPort := netip.AddrPortFrom(addr, port)
				hostAddrs[addrPort.String()] = struct{}{}
			}
			break
		}
	}

	addrs := make([]string, 0, len(hostAddrs))
	for addr := range hostAddrs {
		addrs = append(addrs, addr)
	}
	m.addrs = addrs
	return addrs, nil
}

func (m *MigrationManager) getConn(hostAddr string) (clickhouse.Conn, error) {
	m.addrsMux.Lock()
	defer m.addrsMux.Unlock()
	if conn, ok := m.conns[hostAddr]; ok {
		return conn, nil
	}
	opts := m.connOpts
	opts.Addr = []string{hostAddr}
	conn, err := clickhouse.Open(&opts)
	if err != nil {
		return nil, err
	}
	m.conns[hostAddr] = conn
	return conn, nil
}

func (m *MigrationManager) waitForMutationsOnHost(ctx context.Context, hostAddr string) error {
	// reset backoff
	m.backoff.Reset()

	m.logger.Info("Fetching mutations on host", zap.String("host", hostAddr))
	conn, err := m.getConn(hostAddr)
	if err != nil {
		return err
	}
	for {
		if m.backoff.NextBackOff() == backoff.Stop {
			return errors.New("backoff stopped")
		}
		var mutations []Mutation
		if err := conn.Select(ctx, &mutations, "SELECT database, table, command, mutation_id, latest_fail_reason FROM system.mutations WHERE is_done = 0"); err != nil {
			return err
		}
		if len(mutations) != 0 {
			m.logger.Info("Waiting for mutations to be completed", zap.Int("count", len(mutations)), zap.String("host", hostAddr))
			for _, mutation := range mutations {
				m.logger.Info("Mutation details",
					zap.String("database", mutation.Database),
					zap.String("table", mutation.Table),
					zap.String("command", mutation.Command),
					zap.String("mutation_id", mutation.MutationID),
					zap.String("latest_fail_reason", mutation.LatestFailReason),
				)
			}
			time.Sleep(m.backoff.NextBackOff())
			continue
		}
		m.logger.Info("No mutations found on host", zap.String("host", hostAddr))
		break
	}
	return nil
}

// WaitForRunningMutations waits for all the mutations to be completed on all the hosts in the cluster.
func (m *MigrationManager) WaitForRunningMutations(ctx context.Context) error {
	addrs, err := m.HostAddrs()
	if err != nil {
		return err
	}
	for _, hostAddr := range addrs {
		m.logger.Info("Waiting for mutations on host", zap.String("host", hostAddr))
		if err := m.waitForMutationsOnHost(ctx, hostAddr); err != nil {
			return errors.Join(ErrFailedToWaitForMutations, err)
		}
	}
	return nil
}

// WaitDistributedDDLQueue waits for all the DDLs to be completed on all the hosts in the cluster.
func (m *MigrationManager) WaitDistributedDDLQueue(ctx context.Context) error {
	// In standalone mode, there's no distributed DDL queue
	if m.clusterName == "" {
		m.logger.Info("Standalone mode: skipping distributed DDL queue wait")
		return nil
	}
	// reset backoff
	m.backoff.Reset()
	m.logger.Info("Fetching non-finished DDLs from distributed DDL queue")
	for {
		if m.backoff.NextBackOff() == backoff.Stop {
			return errors.New("backoff stopped")
		}

		ddlQueue, err := m.getDistributedDDLQueue(ctx)
		if err != nil {
			return err
		}

		if len(ddlQueue) != 0 {
			m.logger.Info("Waiting for distributed DDL queue to be completed", zap.Int("count", len(ddlQueue)))
			for _, ddl := range ddlQueue {
				m.logger.Info("DDL details",
					zap.String("query", ddl.Query),
					zap.String("status", ddl.Status),
					zap.String("host", ddl.Host),
					zap.String("exception_code", ddl.ExceptionCode),
				)
			}
			time.Sleep(m.backoff.NextBackOff())
			continue
		}
		m.logger.Info("No pending DDLs found in distributed DDL queue")
		break
	}
	return nil
}

func (m *MigrationManager) getDistributedDDLQueue(ctx context.Context) ([]DistributedDDLQueue, error) {
	var ddlQueue []DistributedDDLQueue
	query := "SELECT entry, cluster, query, host, port, status, exception_code FROM system.distributed_ddl_queue WHERE status != 'Finished'"

	// 10 attempts is an arbitrary number. If we don't get the DDL queue after 10 attempts, we give up.
	for i := 0; i < 10; i++ {
		if err := m.conn.Select(ctx, &ddlQueue, query); err != nil {
			if exception, ok := err.(*clickhouse.Exception); ok {
				if exception.Code == 999 {
					// ClickHouse DDLWorker is cleaning up entries in the distributed_ddl_queue before we can query it. This leads to the exception:
					// code: 999, message: Coordination error: No node, path /clickhouse/signoz-clickhouse/task_queue/ddl/query-000000<some 4 digit number>/finished

					// It looks like this exception is safe to retry on.
					if strings.Contains(exception.Error(), "No node") {
						m.logger.Error("A retryable exception was received while fetching distributed DDL queue", zap.Error(err), zap.Int("attempt", i+1))
						continue
					}
				}
			}

			m.logger.Error("Failed to fetch distributed DDL queue", zap.Error(err), zap.Int("attempt", i+1))
			return nil, err
		}

		// If no exception was thrown, break the loop
		break
	}

	return ddlQueue, nil
}

func (m *MigrationManager) waitForDistributionQueueOnHost(ctx context.Context, conn clickhouse.Conn, db, table string) error {
	errCountQuery := "SELECT count(*) FROM system.distribution_queue WHERE database = $1 AND table = $2 AND (error_count != 0 OR is_blocked = 1)"

	var errCount uint64
	if err := conn.QueryRow(ctx, errCountQuery, db, table).Scan(&errCount); err != nil {
		return errors.Join(ErrFailedToWaitForDistributionQueue, err)
	}

	if errCount != 0 {
		return ErrDistributionQueueError
	}

	query := "SELECT count(*) FROM system.distribution_queue WHERE database = $1 AND table = $2 AND data_files > 0"
	// Should this be configurable and/or higher?
	t := time.NewTimer(2 * time.Minute)
	defer t.Stop()
	minimumInsertsCompletedChan := make(chan struct{})
	errChan := make(chan error)

	// count for the number of inserts in the queue with non-zero data_files
	go func() {
		insertsInQueue := 0
		for {
			var errCount uint64
			if err := conn.QueryRow(ctx, errCountQuery, db, table).Scan(&errCount); err != nil {
				errChan <- errors.Join(ErrFailedToWaitForDistributionQueue, err)
				return
			}
			if errCount != 0 {
				errChan <- ErrDistributionQueueError
				return
			}

			var count uint64
			// if the count of inserts in the queue with non-zero data_files is greater than 0, then it counts towards
			// one insert, while technically it is more than one insert, we are mainly interested in number of such actions
			if err := conn.QueryRow(ctx, query, db, table).Scan(&count); err != nil {
				m.logger.Error("Failed to fetch inserts in queue, will retry", zap.Error(err))
				continue
			}
			if count > 0 {
				insertsInQueue++
			}
			if insertsInQueue >= 16 {
				minimumInsertsCompletedChan <- struct{}{}
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			// we waited for graceful period to complete, it might happen that no inserts occur after the migration
			// so we can't wait forever
			return nil
		case err := <-errChan:
			return err
		case <-minimumInsertsCompletedChan:
			return nil
		}
	}
}

// When dropping a column, we need to make sure that there are no pending items in `distribution_queue`
// for the table. This is because if we drop a column on local table, but there is a pending insert on remote
// table, it will fail.
// There is no deterministic way to find out if there are pending items in `distribution_queue` for a table with
// old schema, so we try to wait for 2 minutes or at least 16 inserts with non-zero `data_files` in the queue
// for the table.
// We need to do this for all hosts in the cluster.
func (m *MigrationManager) WaitForDistributionQueue(ctx context.Context, db, table string) error {
	addrs, err := m.HostAddrs()
	if err != nil {
		return errors.Join(ErrFailedToWaitForDistributionQueue, err)
	}
	for _, hostAddr := range addrs {
		conn, err := m.getConn(hostAddr)
		if err != nil {
			return errors.Join(ErrFailedToWaitForDistributionQueue, err)
		}
		if err := m.waitForDistributionQueueOnHost(ctx, conn, db, table); err != nil {
			return errors.Join(ErrFailedToWaitForDistributionQueue, err)
		}
	}
	return nil
}

func (m *MigrationManager) shouldRunMigration(db string, migrationID uint64, versions []uint64) bool {
	m.logger.Info("Checking if migration should run", zap.String("db", db), zap.Uint64("migration_id", migrationID), zap.Any("versions", versions))
	// if versions are provided, we only run the migrations that are in the versions slice
	if len(versions) != 0 {
		var doesExist bool
		for _, version := range versions {
			if migrationID == version {
				doesExist = true
				break
			}
		}
		if !doesExist {
			m.logger.Info("Migration should not run as it is not in the provided versions", zap.Uint64("migration_id", migrationID), zap.Any("versions", versions))
			return false
		}
	}

	query := fmt.Sprintf("SELECT * FROM %s.schema_migrations_v2 WHERE migration_id = %d SETTINGS final = 1;", db, migrationID)
	m.logger.Info("Fetching migration status", zap.String("query", query))
	var migrationSchemaMigrationRecord MigrationSchemaMigrationRecord
	if err := m.conn.QueryRow(context.Background(), query).ScanStruct(&migrationSchemaMigrationRecord); err != nil {
		if err == sql.ErrNoRows {
			m.logger.Info("Migration not run", zap.Uint64("migration_id", migrationID))
			return true
		}
		// this should not happen
		m.logger.Error("Failed to fetch migration status", zap.Error(err))
		panic(err)
	}
	m.logger.Info("Migration status", zap.Uint64("migration_id", migrationID), zap.String("status", migrationSchemaMigrationRecord.Status))
	if migrationSchemaMigrationRecord.Status != InProgressStatus && migrationSchemaMigrationRecord.Status != FinishedStatus {
		m.logger.Info("Migration not run", zap.Uint64("migration_id", migrationID), zap.String("status", migrationSchemaMigrationRecord.Status))
		return true
	}
	return false
}

// shouldSkipMigrationInStandaloneMode returns true if a migration should be skipped in standalone mode.
// Some migrations add columns (like timestamp) that the OTEL collector doesn't support yet.
// These migrations work fine in clustered mode but break standalone mode with SharedMergeTree.
func (m *MigrationManager) shouldSkipMigrationInStandaloneMode(db string, migrationID uint64) bool {
	if m.clusterName != "" {
		// Not standalone mode, don't skip
		return false
	}

	// Migrations to skip in standalone mode:
	// - signoz_traces migration 1008: Adds timestamp column to span_attributes_keys (OTEL collector doesn't provide it)
	// - signoz_logs migration 1002: Adds timestamp column to logs_attribute_keys and logs_resource_keys
	// - signoz_logs migration 1003: Materializes timestamp column in logs_attribute_keys and logs_resource_keys
	skipMigrations := map[string][]uint64{
		SignozTracesDB: {1008},
		SignozLogsDB:   {1002, 1003},
	}

	if migrationsToSkip, ok := skipMigrations[db]; ok {
		for _, skipID := range migrationsToSkip {
			if migrationID == skipID {
				m.logger.Info("Standalone mode: skipping migration that adds unsupported columns",
					zap.String("db", db),
					zap.Uint64("migration_id", migrationID))
				return true
			}
		}
	}
	return false
}

func (m *MigrationManager) executeSyncOperations(ctx context.Context, operations []Operation, migrationID uint64, db string) error {
	for _, item := range operations {
		if item.ForceMigrate() || (!item.IsMutation() && item.IsIdempotent() && item.IsLightweight()) {
			if err := m.RunOperation(ctx, item, migrationID, db, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *MigrationManager) IsSync(migration SchemaMigrationRecord) bool {
	for _, item := range migration.UpItems {
		// if any of the operations is a sync operation, return true
		if item.ForceMigrate() || (!item.IsMutation() && item.IsIdempotent() && item.IsLightweight()) {
			return true
		}
	}

	return false
}

func (m *MigrationManager) IsAsync(migration SchemaMigrationRecord) bool {
	for _, item := range migration.UpItems {
		// if any of the operations is a force migrate operation, return false
		if item.ForceMigrate() {
			return false
		}

		// If any of the operations is sync, return false
		if !item.IsMutation() && item.IsIdempotent() && item.IsLightweight() {
			return false
		}

		// If any of the operations is not idempotent, return false
		if !item.IsIdempotent() {
			return false
		}
	}

	return true
}

// MigrateUpSync migrates the schema up.
func (m *MigrationManager) MigrateUpSync(ctx context.Context, upVersions []uint64) error {
	m.logger.Info("Running migrations up sync")
	for _, migration := range TracesMigrations {
		if !m.shouldRunMigration(SignozTracesDB, migration.MigrationID, upVersions) {
			continue
		}
		if m.shouldSkipMigrationInStandaloneMode(SignozTracesDB, migration.MigrationID) {
			continue
		}
		if err := m.executeSyncOperations(ctx, migration.UpItems, migration.MigrationID, SignozTracesDB); err != nil {
			return err
		}
	}

	logsMigrations := LogsMigrations
	if constants.EnableLogsMigrationsV2 {
		logsMigrations = LogsMigrationsV2
	}

	for _, migration := range logsMigrations {
		if !m.shouldRunMigration(SignozLogsDB, migration.MigrationID, upVersions) {
			continue
		}
		if m.shouldSkipMigrationInStandaloneMode(SignozLogsDB, migration.MigrationID) {
			continue
		}
		if err := m.executeSyncOperations(ctx, migration.UpItems, migration.MigrationID, SignozLogsDB); err != nil {
			return err
		}
	}

	for _, migration := range MetricsMigrations {
		if !m.shouldRunMigration(SignozMetricsDB, migration.MigrationID, upVersions) {
			continue
		}
		if err := m.executeSyncOperations(ctx, migration.UpItems, migration.MigrationID, SignozMetricsDB); err != nil {
			return err
		}
	}

	for _, migration := range MetadataMigrations {
		if !m.shouldRunMigration(SignozMetadataDB, migration.MigrationID, upVersions) {
			continue
		}
		if err := m.executeSyncOperations(ctx, migration.UpItems, migration.MigrationID, SignozMetadataDB); err != nil {
			return err
		}
	}

	for _, migration := range AnalyticsMigrations {
		if !m.shouldRunMigration(SignozAnalyticsDB, migration.MigrationID, upVersions) {
			continue
		}
		if err := m.executeSyncOperations(ctx, migration.UpItems, migration.MigrationID, SignozAnalyticsDB); err != nil {
			return err
		}
	}

	for _, migration := range MeterMigrations {
		if !m.shouldRunMigration(SignozMeterDB, migration.MigrationID, upVersions) {
			continue
		}
		if err := m.executeSyncOperations(ctx, migration.UpItems, migration.MigrationID, SignozMeterDB); err != nil {
			return err
		}
	}

	// In standalone mode, distributed_* tables are created as VIEWs during table creation,
	// so no additional conversion is needed. Base tables remain as SharedMergeTree for OTEL writes.

	return nil
}

// MigrateDownSync migrates the schema down.
func (m *MigrationManager) MigrateDownSync(ctx context.Context, downVersions []uint64) error {

	m.logger.Info("Running migrations down sync")

	for _, migration := range TracesMigrations {
		if !m.shouldRunMigration(SignozTracesDB, migration.MigrationID, downVersions) {
			continue
		}
		if m.shouldSkipMigrationInStandaloneMode(SignozTracesDB, migration.MigrationID) {
			continue
		}
		if err := m.executeSyncOperations(ctx, migration.DownItems, migration.MigrationID, SignozTracesDB); err != nil {
			return err
		}
	}

	logsMigrations := LogsMigrations
	if constants.EnableLogsMigrationsV2 {
		logsMigrations = LogsMigrationsV2
	}

	for _, migration := range logsMigrations {
		if !m.shouldRunMigration(SignozLogsDB, migration.MigrationID, downVersions) {
			continue
		}
		if m.shouldSkipMigrationInStandaloneMode(SignozLogsDB, migration.MigrationID) {
			continue
		}
		if err := m.executeSyncOperations(ctx, migration.DownItems, migration.MigrationID, SignozLogsDB); err != nil {
			return err
		}
	}

	for _, migration := range MetricsMigrations {
		if !m.shouldRunMigration(SignozMetricsDB, migration.MigrationID, downVersions) {
			continue
		}
		if err := m.executeSyncOperations(ctx, migration.DownItems, migration.MigrationID, SignozMetricsDB); err != nil {
			return err
		}
	}

	for _, migration := range AnalyticsMigrations {
		if !m.shouldRunMigration(SignozAnalyticsDB, migration.MigrationID, downVersions) {
			continue
		}
		if err := m.executeSyncOperations(ctx, migration.DownItems, migration.MigrationID, SignozAnalyticsDB); err != nil {
			return err
		}
	}

	for _, migration := range MeterMigrations {
		if !m.shouldRunMigration(SignozMeterDB, migration.MigrationID, downVersions) {
			continue
		}
		if err := m.executeSyncOperations(ctx, migration.DownItems, migration.MigrationID, SignozMeterDB); err != nil {
			return err
		}
	}

	return nil
}

// MigrateUpAsync migrates the schema up.
func (m *MigrationManager) MigrateUpAsync(ctx context.Context, upVersions []uint64) error {

	m.logger.Info("Running migrations up async")
	for _, migration := range TracesMigrations {
		if !m.shouldRunMigration(SignozTracesDB, migration.MigrationID, upVersions) {
			continue
		}
		if m.shouldSkipMigrationInStandaloneMode(SignozTracesDB, migration.MigrationID) {
			continue
		}
		for _, item := range migration.UpItems {
			if item.ForceMigrate() {
				m.logger.Info("Skipping force sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the force sync operation (should run in sync mode)
				continue
			}
			if !item.IsMutation() && item.IsIdempotent() && item.IsLightweight() {
				m.logger.Info("Skipping sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the sync operation
				continue
			}
			if item.IsIdempotent() {
				if err := m.RunOperation(ctx, item, migration.MigrationID, SignozTracesDB, false); err != nil {
					return err
				}
			}
		}
	}

	for _, migration := range MetricsMigrations {
		if !m.shouldRunMigration(SignozMetricsDB, migration.MigrationID, upVersions) {
			continue
		}
		for _, item := range migration.UpItems {
			if item.ForceMigrate() {
				m.logger.Info("Skipping force sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the force sync operation (should run in sync mode)
				continue
			}
			if !item.IsMutation() && item.IsIdempotent() && item.IsLightweight() {
				m.logger.Info("Skipping sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the sync operation
				continue
			}
			if item.IsIdempotent() {
				if err := m.RunOperation(ctx, item, migration.MigrationID, SignozMetricsDB, false); err != nil {
					return err
				}
			}
		}
	}

	logsMigrations := LogsMigrations
	if constants.EnableLogsMigrationsV2 {
		logsMigrations = LogsMigrationsV2
	}

	for _, migration := range logsMigrations {
		if !m.shouldRunMigration(SignozLogsDB, migration.MigrationID, upVersions) {
			continue
		}
		if m.shouldSkipMigrationInStandaloneMode(SignozLogsDB, migration.MigrationID) {
			continue
		}
		for _, item := range migration.UpItems {
			if item.ForceMigrate() {
				m.logger.Info("Skipping force sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the force sync operation (should run in sync mode)
				continue
			}
			if !item.IsMutation() && item.IsIdempotent() && item.IsLightweight() {
				m.logger.Info("Skipping sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the sync operation
				continue
			}
			if item.IsIdempotent() {
				if err := m.RunOperation(ctx, item, migration.MigrationID, SignozLogsDB, false); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// MigrateDownAsync migrates the schema down.
func (m *MigrationManager) MigrateDownAsync(ctx context.Context, downVersions []uint64) error {

	m.logger.Info("Running migrations down async")

	for _, migration := range TracesMigrations {
		if !m.shouldRunMigration(SignozTracesDB, migration.MigrationID, downVersions) {
			continue
		}
		if m.shouldSkipMigrationInStandaloneMode(SignozTracesDB, migration.MigrationID) {
			continue
		}
		for _, item := range migration.DownItems {
			if item.ForceMigrate() {
				m.logger.Info("Skipping force sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the force sync operation (should run in sync mode)
				continue
			}
			if !item.IsMutation() && item.IsIdempotent() && item.IsLightweight() {
				m.logger.Info("Skipping sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the sync operation
				continue
			}
			if item.IsIdempotent() {
				if err := m.RunOperation(ctx, item, migration.MigrationID, SignozTracesDB, false); err != nil {
					return err
				}
			}
		}
	}

	for _, migration := range MetricsMigrations {
		if !m.shouldRunMigration(SignozMetricsDB, migration.MigrationID, downVersions) {
			continue
		}
		for _, item := range migration.DownItems {
			if item.ForceMigrate() {
				m.logger.Info("Skipping force sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the force sync operation (should run in sync mode)
				continue
			}
			if !item.IsMutation() && item.IsIdempotent() && item.IsLightweight() {
				m.logger.Info("Skipping sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the sync operation
				continue
			}
			if item.IsIdempotent() {
				if err := m.RunOperation(ctx, item, migration.MigrationID, SignozMetricsDB, false); err != nil {
					return err
				}
			}
		}
	}

	for _, migration := range LogsMigrations {
		if !m.shouldRunMigration(SignozLogsDB, migration.MigrationID, downVersions) {
			continue
		}
		if m.shouldSkipMigrationInStandaloneMode(SignozLogsDB, migration.MigrationID) {
			continue
		}
		for _, item := range migration.DownItems {
			if item.ForceMigrate() {
				m.logger.Info("Skipping force sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the force sync operation (should run in sync mode)
				continue
			}
			if !item.IsMutation() && item.IsIdempotent() && item.IsLightweight() {
				m.logger.Info("Skipping sync operation", zap.Uint64("migration_id", migration.MigrationID))
				// skip the sync operation
				continue
			}
			if item.IsIdempotent() {
				if err := m.RunOperation(ctx, item, migration.MigrationID, SignozLogsDB, false); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (m *MigrationManager) insertMigrationEntry(ctx context.Context, db string, migrationID uint64, status string) error {
	// In standalone mode, use local table instead of distributed table
	migrationsTable := "distributed_schema_migrations_v2"
	if m.clusterName == "" {
		migrationsTable = "schema_migrations_v2"
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (migration_id, status, created_at) VALUES (%d, '%s', '%s')", db, migrationsTable, migrationID, status, time.Now().UTC().Format("2006-01-02 15:04:05"))
	m.logger.Info("Inserting migration entry", zap.String("query", query))
	return m.conn.Exec(ctx, query)
}

func (m *MigrationManager) updateMigrationEntry(ctx context.Context, db string, migrationID uint64, status string, errStr string) error {
	var query string
	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	// Use interpolated values instead of placeholders - ClickHouse doesn't support $1, $2 style placeholders in ALTER TABLE
	if m.clusterName != "" {
		query = fmt.Sprintf("ALTER TABLE %s.schema_migrations_v2 ON CLUSTER %s UPDATE status = '%s', error = '%s', updated_at = '%s' WHERE migration_id = %d", db, m.clusterName, status, errStr, now, migrationID)
	} else {
		// Standalone mode: no ON CLUSTER
		query = fmt.Sprintf("ALTER TABLE %s.schema_migrations_v2 UPDATE status = '%s', error = '%s', updated_at = '%s' WHERE migration_id = %d", db, status, errStr, now, migrationID)
	}
	m.logger.Info("Updating migration entry", zap.String("query", query), zap.String("status", status), zap.String("error", errStr), zap.Uint64("migration_id", migrationID))
	return m.conn.Exec(ctx, query)
}

func (m *MigrationManager) RunOperation(ctx context.Context, operation Operation, migrationID uint64, database string, skipStatusUpdate bool) error {
	m.logger.Info("Running operation", zap.Uint64("migration_id", migrationID), zap.String("database", database), zap.Bool("skip_status_update", skipStatusUpdate))
	start := time.Now()
	var sql string
	if m.clusterName != "" {
		operation = operation.OnCluster(m.clusterName)
	}
	if m.replicationEnabled {
		operation = operation.WithReplication()
	}

	// In standalone mode, skip ALTER TABLE operations on tables that have been converted to VIEWs
	// The distributed_* table gets altered anyway, and the VIEW auto-reflects schema changes
	if m.clusterName == "" {
		var alterDB, alterTable string
		switch op := operation.(type) {
		case *AlterTableAddColumn:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableAddColumn:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableDropColumn:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableDropColumn:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifyColumn:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifyColumn:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifyTTL:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifyTTL:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableDropTTL:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableDropTTL:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifySettings:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifySettings:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableAddIndex:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableAddIndex:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableDropIndex:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableDropIndex:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableMaterializeIndex:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableMaterializeIndex:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableClearIndex:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableClearIndex:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifyColumnRemove:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifyColumnRemove:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifyColumnModifySettings:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifyColumnModifySettings:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifyColumnResetSettings:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifyColumnResetSettings:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableMaterializeColumn:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableMaterializeColumn:
			alterDB, alterTable = op.Database, op.Table
		}

		if alterDB != "" && alterTable != "" {
			// In standalone mode, distributed_* tables are VIEWs that use SELECT *
			// They auto-reflect any column changes from the base table, so skip ALTERs
			if m.clusterName == "" && strings.HasPrefix(alterTable, "distributed_") {
				m.logger.Info("Standalone mode: skipping ALTER on distributed_* VIEW (auto-reflects base table)",
					zap.String("table", alterDB+"."+alterTable),
					zap.Uint64("migration_id", migrationID))
				return nil
			}

			// Check if this table was actually converted to a VIEW
			// Only redirect ALTERs for actual VIEWs, not rollup tables that have distributed counterparts
			tableKey := alterDB + "." + alterTable
			if m.convertedToViews[tableKey] {
				distTable := m.baseToDistributed[tableKey]
				m.logger.Info("Standalone mode: redirecting ALTER from VIEW to distributed table",
					zap.String("view", tableKey),
					zap.String("distributed", distTable),
					zap.Uint64("migration_id", migrationID))

				// Redirect the ALTER to the distributed table
				distParts := strings.Split(distTable, ".")
				if len(distParts) == 2 {
					// Create a new operation targeting the distributed table
					var redirectedOp Operation
					switch op := operation.(type) {
					case *AlterTableAddColumn:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableAddColumn:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableDropColumn:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableDropColumn:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifyColumn:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifyColumn:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifyTTL:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifyTTL:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableDropTTL:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableDropTTL:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifySettings:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifySettings:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableAddIndex:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableAddIndex:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableDropIndex:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableDropIndex:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableMaterializeIndex:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableMaterializeIndex:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableClearIndex:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableClearIndex:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifyColumnRemove:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifyColumnRemove:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifyColumnModifySettings:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifyColumnModifySettings:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifyColumnResetSettings:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifyColumnResetSettings:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableMaterializeColumn:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableMaterializeColumn:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					default:
						redirectedOp = operation
					}

					// Execute the ALTER on the distributed table
					sql = redirectedOp.ToSQL()
					m.logger.Info("Executing ALTER on distributed table", zap.String("sql", sql))
					if err := m.conn.Exec(ctx, sql); err != nil {
						m.logger.Error("Failed to ALTER distributed table", zap.Error(err))
						if !skipStatusUpdate {
							if err := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error()); err != nil {
								return err
							}
						}
						return err
					}

					// Recreate the VIEW to pick up new columns
					m.logger.Info("Recreating VIEW to pick up schema changes", zap.String("view", tableKey))
					dropViewSQL := fmt.Sprintf("DROP VIEW IF EXISTS %s.%s", alterDB, alterTable)
					if err := m.conn.Exec(ctx, dropViewSQL); err != nil {
						m.logger.Error("Failed to drop VIEW", zap.Error(err), zap.String("view", tableKey))
						// Continue anyway - the VIEW might not exist
					}
					createViewSQL := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s",
						alterDB, alterTable, distTable)
					if err := m.conn.Exec(ctx, createViewSQL); err != nil {
						m.logger.Error("Failed to recreate VIEW", zap.Error(err), zap.String("view", tableKey))
						if !skipStatusUpdate {
							if err := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error()); err != nil {
								return err
							}
						}
						return err
					}

					// Mark migration as complete
					if !skipStatusUpdate {
						if err := m.insertMigrationEntry(ctx, database, migrationID, FinishedStatus); err != nil {
							return err
						}
					}
					return nil
				}
			}
		}
	}

	// In standalone mode, handle table creation specially
	if m.clusterName == "" {
		// Try to get CreateTableOperation (check both pointer and value types)
		var createOp *CreateTableOperation
		if cop, ok := operation.(*CreateTableOperation); ok {
			createOp = cop
		} else if cop, ok := operation.(CreateTableOperation); ok {
			createOp = &cop
		}

		if createOp != nil {
			// Get the Distributed engine (check both pointer and value types)
			var distEngine *Distributed
			if de, ok := createOp.Engine.(*Distributed); ok {
				distEngine = de
			} else if de, ok := createOp.Engine.(Distributed); ok {
				distEngine = &de
			}

		if distEngine != nil {
				// Handle Distributed table in standalone mode:
				// OTEL collector writes to distributed_* tables, so these must be real storage.
				// Pattern: distributed_* = SharedMergeTree, base = VIEW pointing to distributed_*
				distributedTableKey := createOp.Database + "." + createOp.Table
				baseTableKey := distEngine.BaseTableDatabase() + "." + distEngine.BaseTableName()
				baseDatabase := distEngine.BaseTableDatabase()
				baseTable := distEngine.BaseTableName()

				m.logger.Info("Standalone mode: creating distributed table as SharedMergeTree",
					zap.String("distributed_table", distributedTableKey),
					zap.String("base_table", baseTableKey))

				// Look up the base table engine info (should have been stored earlier)
				if baseInfo, ok := m.baseTableEngines[baseTableKey]; ok {
					// Create SharedMergeTree with the distributed_ name using base table's engine
					sharedMergeTreeSQL := m.generateStandaloneTableSQL(createOp.Database, createOp.Table, baseInfo.Columns, baseInfo.Indexes, baseInfo.Projections, baseInfo.Engine)
					m.logger.Info("Standalone mode: generated SharedMergeTree SQL for distributed table", zap.String("sql", sharedMergeTreeSQL))

					// Also create the VIEW for the base table (since we skipped base table creation earlier)
					viewSQL := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s",
						baseDatabase, baseTable, createOp.Database, createOp.Table)
					m.logger.Info("Standalone mode: generated VIEW SQL for base table", zap.String("sql", viewSQL))

					// Execute both: first create the SharedMergeTree, then the VIEW
					// We'll execute the VIEW creation inline here since the main execution only handles one SQL
					sql = sharedMergeTreeSQL

					// Track for view creation after main execution
					m.pendingBaseTableViews[baseTableKey] = distributedTableKey
					m.convertedToViews[distributedTableKey] = true
					m.convertedToViews[baseTableKey] = true
				} else {
					// Base table not seen yet - create SharedMergeTree and hope base comes later
					m.logger.Warn("Standalone mode: base table not found, creating distributed table as VIEW fallback",
						zap.String("distributed_table", distributedTableKey),
						zap.String("base_table", baseTableKey))
					sql = fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s",
						createOp.Database, createOp.Table, baseTableKey)
				}

				// Track the relationship
				m.baseToDistributed[baseTableKey] = distributedTableKey
			} else {
				// Non-distributed table: check if it should be a VIEW (if distributed was already processed)
				tableKey := createOp.Database + "." + createOp.Table

				// Special case: schema_migrations_v2 must be a real table (migrator uses ALTER TABLE on it)
				if createOp.Table == "schema_migrations_v2" {
					m.logger.Info("Standalone mode: creating schema_migrations_v2 as real table (required for mutations)",
						zap.String("table", tableKey))
					sql = m.generateStandaloneTableSQL(createOp.Database, createOp.Table, createOp.Columns, createOp.Indexes, createOp.Projections, createOp.Engine)
				} else if distributedTableKey, isPendingView := m.pendingBaseTableViews[tableKey]; isPendingView {
					// This base table should be created as VIEW pointing to the distributed table
					m.logger.Info("Standalone mode: creating base table as VIEW",
						zap.String("base_table", tableKey),
						zap.String("pointing_to", distributedTableKey))
					sql = fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s",
						createOp.Database, createOp.Table, distributedTableKey)
					m.convertedToViews[tableKey] = true
					delete(m.pendingBaseTableViews, tableKey) // Consumed
				} else {
					// Store engine info for later use when we see the distributed table
					m.baseTableEngines[tableKey] = BaseTableInfo{
						Database:    createOp.Database,
						Table:       createOp.Table,
						Engine:      createOp.Engine,
						Columns:     createOp.Columns,
						Indexes:     createOp.Indexes,
						Projections: createOp.Projections,
					}
					m.logger.Info("Standalone mode: tracked base table, deferring creation",
						zap.String("table", tableKey))

					// Don't create anything yet - wait for the distributed table
					// Use special marker to skip this operation entirely
					sql = "-- STANDALONE_SKIP_BASE_TABLE"
				}
			}
		}
	}

	// In standalone mode, distributed_* tables are now the real storage (SharedMergeTree)
	// INSERTs to distributed_* tables go directly to them, no redirection needed

	// Handle Materialized Views that need to write TO a deferred or VIEW base table
	// Case 1: Target was deferred - create it now as SharedMergeTree
	// Case 2: Target is a VIEW - redirect MV to write to the underlying distributed_ table
	if m.clusterName == "" {
		var mvOp *CreateMaterializedViewOperation
		if op, ok := operation.(*CreateMaterializedViewOperation); ok {
			mvOp = op
		} else if op, ok := operation.(CreateMaterializedViewOperation); ok {
			mvOp = &op
		}
		if mvOp != nil && mvOp.DestTable != "" {
			destTableKey := mvOp.Database + "." + mvOp.DestTable
			if baseInfo, isDeferred := m.baseTableEngines[destTableKey]; isDeferred {
				// Case 1: The MV target was deferred - create it now as SharedMergeTree
				m.logger.Info("Standalone mode: creating deferred table for MV target",
					zap.String("dest_table", destTableKey),
					zap.String("mv", mvOp.ViewName))
				createSQL := m.generateStandaloneTableSQL(baseInfo.Database, baseInfo.Table, baseInfo.Columns, baseInfo.Indexes, baseInfo.Projections, baseInfo.Engine)
				if err := m.conn.Exec(ctx, createSQL); err != nil {
					m.logger.Error("Standalone mode: failed to create MV target table",
						zap.String("dest_table", destTableKey),
						zap.Error(err))
					updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
					if updateErr != nil {
						return errors.Join(err, updateErr)
					}
					return err
				}
				// Remove from deferred list since we created it
				delete(m.baseTableEngines, destTableKey)
			} else if distributedTableKey, isView := m.baseToDistributed[destTableKey]; isView {
				// Case 2: The MV target is a VIEW - redirect to the distributed_ table
				// MVs cannot write TO VIEWs, they need to write to real tables
				m.logger.Info("Standalone mode: redirecting MV target from VIEW to distributed table",
					zap.String("original_dest", destTableKey),
					zap.String("redirected_dest", distributedTableKey),
					zap.String("mv", mvOp.ViewName))
				// Parse the distributed table key to get database and table
				parts := strings.SplitN(distributedTableKey, ".", 2)
				if len(parts) == 2 {
					mvOp.DestTable = parts[1]
					// Update sql to use the redirected MV
					sql = mvOp.ToSQL()
				}
			}
		}
	}

	m.logger.Info("Waiting for running mutations before running the operation")

	if err := m.WaitForRunningMutations(ctx); err != nil {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
		if updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}
	if err := m.WaitDistributedDDLQueue(ctx); err != nil {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
		if updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}

	if shouldWaitForDistributionQueue, database, table := operation.ShouldWaitForDistributionQueue(); shouldWaitForDistributionQueue {
		m.logger.Info("Waiting for distribution queue", zap.String("database", database), zap.String("table", table))
		if err := m.WaitForDistributionQueue(ctx, database, table); err != nil {
			updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
			if updateErr != nil {
				return errors.Join(err, updateErr)
			}
			return err
		}
	}

	if !skipStatusUpdate {
		insertErr := m.insertMigrationEntry(ctx, database, migrationID, InProgressStatus)
		if insertErr != nil {
			return insertErr
		}
	}

	// If sql wasn't set by standalone mode handling, generate it normally
	if sql == "" {
		sql = operation.ToSQL()
	}

	// Skip operations that are deferred in standalone mode (base tables waiting for distributed)
	if sql == "-- STANDALONE_SKIP_BASE_TABLE" {
		m.logger.Info("Standalone mode: skipping base table creation (will be created as VIEW later)")
		if !skipStatusUpdate {
			updateErr := m.updateMigrationEntry(ctx, database, migrationID, FinishedStatus, "")
			if updateErr != nil {
				return updateErr
			}
		}
		return nil
	}

	m.logger.Info("Running operation", zap.String("sql", sql))
	err := m.conn.Exec(ctx, sql)
	if err != nil {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
		if updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}

	// In standalone mode, create pending VIEW for base tables after SharedMergeTree is created
	if m.clusterName == "" && len(m.pendingBaseTableViews) > 0 {
		for baseTableKey, distributedTableKey := range m.pendingBaseTableViews {
			// Parse base table key to get database and table
			parts := strings.SplitN(baseTableKey, ".", 2)
			if len(parts) != 2 {
				continue
			}
			baseDB, baseTable := parts[0], parts[1]

			// Special case: schema_migrations_v2 must be a real table (migrator uses ALTER TABLE on it)
			// Skip creating a VIEW for it - the actual table was already created
			if baseTable == "schema_migrations_v2" {
				m.logger.Info("Standalone mode: skipping VIEW creation for schema_migrations_v2 (needs to be real table)",
					zap.String("base_table", baseTableKey))
				delete(m.pendingBaseTableViews, baseTableKey)
				continue
			}

			viewSQL := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s",
				baseDB, baseTable, distributedTableKey)
			m.logger.Info("Standalone mode: creating VIEW for base table",
				zap.String("base_table", baseTableKey),
				zap.String("pointing_to", distributedTableKey),
				zap.String("sql", viewSQL))

			if viewErr := m.conn.Exec(ctx, viewSQL); viewErr != nil {
				m.logger.Error("Standalone mode: failed to create VIEW for base table",
					zap.String("base_table", baseTableKey),
					zap.Error(viewErr))
				updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, viewErr.Error())
				if updateErr != nil {
					return errors.Join(viewErr, updateErr)
				}
				return viewErr
			}

			// Mark as consumed - remove from both pending lists
			delete(m.pendingBaseTableViews, baseTableKey)
			delete(m.baseTableEngines, baseTableKey) // Also remove from engines so MV handling doesn't try to create it
		}
	}

	m.logger.Info("Waiting for running mutations after running the operation")

	if err := m.WaitForRunningMutations(ctx); err != nil {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
		if updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}
	if err := m.WaitDistributedDDLQueue(ctx); err != nil {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FailedStatus, err.Error())
		if updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}
	if !skipStatusUpdate {
		updateErr := m.updateMigrationEntry(ctx, database, migrationID, FinishedStatus, "")
		if updateErr != nil {
			return updateErr
		}
	}
	duration := time.Since(start)
	m.logger.Info("Operation completed", zap.Uint64("migration_id", migrationID), zap.String("database", database), zap.Duration("duration", duration))

	return nil
}

func (m *MigrationManager) RunOperationWithoutUpdate(ctx context.Context, operation Operation, migrationID uint64, database string) error {
	m.logger.Info("Running operation", zap.Uint64("migration_id", migrationID), zap.String("database", database))
	start := time.Now()

	var sql string
	if m.clusterName != "" {
		operation = operation.OnCluster(m.clusterName)
	}

	if m.replicationEnabled {
		operation = operation.WithReplication()
	}

	// In standalone mode, skip ALTER TABLE operations on tables that have been converted to VIEWs
	if m.clusterName == "" {
		var alterDB, alterTable string
		switch op := operation.(type) {
		case *AlterTableAddColumn:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableAddColumn:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableDropColumn:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableDropColumn:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifyColumn:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifyColumn:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifyTTL:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifyTTL:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableDropTTL:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableDropTTL:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifySettings:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifySettings:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableAddIndex:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableAddIndex:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableDropIndex:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableDropIndex:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableMaterializeIndex:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableMaterializeIndex:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableClearIndex:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableClearIndex:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifyColumnRemove:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifyColumnRemove:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifyColumnModifySettings:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifyColumnModifySettings:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableModifyColumnResetSettings:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableModifyColumnResetSettings:
			alterDB, alterTable = op.Database, op.Table
		case *AlterTableMaterializeColumn:
			alterDB, alterTable = op.Database, op.Table
		case AlterTableMaterializeColumn:
			alterDB, alterTable = op.Database, op.Table
		}

		if alterDB != "" && alterTable != "" {
			// In standalone mode, distributed_* tables are VIEWs that use SELECT *
			// They auto-reflect any column changes from the base table, so skip ALTERs
			if m.clusterName == "" && strings.HasPrefix(alterTable, "distributed_") {
				m.logger.Info("Standalone mode: skipping ALTER on distributed_* VIEW (auto-reflects base table)",
					zap.String("table", alterDB+"."+alterTable),
					zap.Uint64("migration_id", migrationID))
				return nil
			}

			// Check if this table was actually converted to a VIEW
			// Only redirect ALTERs for actual VIEWs, not rollup tables that have distributed counterparts
			tableKey := alterDB + "." + alterTable
			if m.convertedToViews[tableKey] {
				distTable := m.baseToDistributed[tableKey]
				m.logger.Info("Standalone mode: redirecting ALTER from VIEW to distributed table",
					zap.String("view", tableKey),
					zap.String("distributed", distTable),
					zap.Uint64("migration_id", migrationID))

				// Redirect the ALTER to the distributed table
				distParts := strings.Split(distTable, ".")
				if len(distParts) == 2 {
					// Create a new operation targeting the distributed table
					var redirectedOp Operation
					switch op := operation.(type) {
					case *AlterTableAddColumn:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableAddColumn:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableDropColumn:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableDropColumn:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifyColumn:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifyColumn:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifyTTL:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifyTTL:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableDropTTL:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableDropTTL:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifySettings:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifySettings:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableAddIndex:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableAddIndex:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableDropIndex:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableDropIndex:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableMaterializeIndex:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableMaterializeIndex:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableClearIndex:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableClearIndex:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifyColumnRemove:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifyColumnRemove:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifyColumnModifySettings:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifyColumnModifySettings:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableModifyColumnResetSettings:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableModifyColumnResetSettings:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					case *AlterTableMaterializeColumn:
						newOp := *op
						newOp.Database = distParts[0]
						newOp.Table = distParts[1]
						redirectedOp = &newOp
					case AlterTableMaterializeColumn:
						op.Database = distParts[0]
						op.Table = distParts[1]
						redirectedOp = op
					default:
						redirectedOp = operation
					}

					// Execute the ALTER on the distributed table
					sql = redirectedOp.ToSQL()
					m.logger.Info("Executing ALTER on distributed table", zap.String("sql", sql))
					if err := m.conn.Exec(ctx, sql); err != nil {
						m.logger.Error("Failed to ALTER distributed table", zap.Error(err))
						return err
					}

					// Recreate the VIEW to pick up new columns
					m.logger.Info("Recreating VIEW to pick up schema changes", zap.String("view", tableKey))
					dropViewSQL := fmt.Sprintf("DROP VIEW IF EXISTS %s.%s", alterDB, alterTable)
					if err := m.conn.Exec(ctx, dropViewSQL); err != nil {
						m.logger.Error("Failed to drop VIEW", zap.Error(err), zap.String("view", tableKey))
						// Continue anyway - the VIEW might not exist
					}
					createViewSQL := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s",
						alterDB, alterTable, distTable)
					if err := m.conn.Exec(ctx, createViewSQL); err != nil {
						m.logger.Error("Failed to recreate VIEW", zap.Error(err), zap.String("view", tableKey))
						return err
					}

					return m.InsertMigrationEntry(ctx, database, migrationID, FinishedStatus)
				}
			}
		}
	}

	// In standalone mode, handle table creation specially
	if m.clusterName == "" {
		// Try to get CreateTableOperation (check both pointer and value types)
		var createOp *CreateTableOperation
		if cop, ok := operation.(*CreateTableOperation); ok {
			createOp = cop
		} else if cop, ok := operation.(CreateTableOperation); ok {
			createOp = &cop
		}

		if createOp != nil {
			// Get the Distributed engine (check both pointer and value types)
			var distEngine *Distributed
			if de, ok := createOp.Engine.(*Distributed); ok {
				distEngine = de
			} else if de, ok := createOp.Engine.(Distributed); ok {
				distEngine = &de
			}

			if distEngine != nil {
				// Handle Distributed table in standalone mode:
				// OTEL collector writes to distributed_* tables, so these must be real storage.
				// Pattern: distributed_* = SharedMergeTree, base = VIEW pointing to distributed_*
				distributedTableKey := createOp.Database + "." + createOp.Table
				baseTableKey := distEngine.BaseTableDatabase() + "." + distEngine.BaseTableName()
				baseDatabase := distEngine.BaseTableDatabase()
				baseTable := distEngine.BaseTableName()

				m.logger.Info("Standalone mode: creating distributed table as SharedMergeTree",
					zap.String("distributed_table", distributedTableKey),
					zap.String("base_table", baseTableKey))

				// Look up the base table engine info (should have been stored earlier)
				if baseInfo, ok := m.baseTableEngines[baseTableKey]; ok {
					// Create SharedMergeTree with the distributed_ name using base table's engine
					sharedMergeTreeSQL := m.generateStandaloneTableSQL(createOp.Database, createOp.Table, baseInfo.Columns, baseInfo.Indexes, baseInfo.Projections, baseInfo.Engine)
					m.logger.Info("Standalone mode: generated SharedMergeTree SQL for distributed table", zap.String("sql", sharedMergeTreeSQL))

					// Also create the VIEW for the base table (since we skipped base table creation earlier)
					viewSQL := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s",
						baseDatabase, baseTable, createOp.Database, createOp.Table)
					m.logger.Info("Standalone mode: generated VIEW SQL for base table", zap.String("sql", viewSQL))

					sql = sharedMergeTreeSQL

					// Track for view creation after main execution
					m.pendingBaseTableViews[baseTableKey] = distributedTableKey
					m.convertedToViews[distributedTableKey] = true
					m.convertedToViews[baseTableKey] = true
				} else {
					// Base table not seen yet - create SharedMergeTree and hope base comes later
					m.logger.Warn("Standalone mode: base table not found, creating distributed table as VIEW fallback",
						zap.String("distributed_table", distributedTableKey),
						zap.String("base_table", baseTableKey))
					sql = fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s",
						createOp.Database, createOp.Table, baseTableKey)
				}

				// Track the relationship
				m.baseToDistributed[baseTableKey] = distributedTableKey
			} else {
				// Non-distributed table: check if it should be a VIEW (if distributed was already processed)
				tableKey := createOp.Database + "." + createOp.Table

				// Special case: schema_migrations_v2 must be a real table (migrator uses ALTER TABLE on it)
				if createOp.Table == "schema_migrations_v2" {
					m.logger.Info("Standalone mode: creating schema_migrations_v2 as real table (required for mutations)",
						zap.String("table", tableKey))
					sql = m.generateStandaloneTableSQL(createOp.Database, createOp.Table, createOp.Columns, createOp.Indexes, createOp.Projections, createOp.Engine)
				} else if distributedTableKey, isPendingView := m.pendingBaseTableViews[tableKey]; isPendingView {
					// This base table should be created as VIEW pointing to the distributed table
					m.logger.Info("Standalone mode: creating base table as VIEW",
						zap.String("base_table", tableKey),
						zap.String("pointing_to", distributedTableKey))
					sql = fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s",
						createOp.Database, createOp.Table, distributedTableKey)
					m.convertedToViews[tableKey] = true
					delete(m.pendingBaseTableViews, tableKey) // Consumed
				} else {
					// Store engine info for later use when we see the distributed table
					m.baseTableEngines[tableKey] = BaseTableInfo{
						Database:    createOp.Database,
						Table:       createOp.Table,
						Engine:      createOp.Engine,
						Columns:     createOp.Columns,
						Indexes:     createOp.Indexes,
						Projections: createOp.Projections,
					}
					m.logger.Info("Standalone mode: tracked base table, deferring creation",
						zap.String("table", tableKey))

					// Don't create anything yet - wait for the distributed table
					// Use special marker to skip this operation entirely
					sql = "-- STANDALONE_SKIP_BASE_TABLE"
				}
			}
		}
	}

	// In standalone mode, distributed_* tables are now the real storage (SharedMergeTree)
	// INSERTs to distributed_* tables go directly to them, no redirection needed

	// Handle Materialized Views that need to write TO a deferred or VIEW base table
	// Case 1: Target was deferred - create it now as SharedMergeTree
	// Case 2: Target is a VIEW - redirect MV to write to the underlying distributed_ table
	if m.clusterName == "" {
		var mvOp *CreateMaterializedViewOperation
		if op, ok := operation.(*CreateMaterializedViewOperation); ok {
			mvOp = op
		} else if op, ok := operation.(CreateMaterializedViewOperation); ok {
			mvOp = &op
		}
		if mvOp != nil && mvOp.DestTable != "" {
			destTableKey := mvOp.Database + "." + mvOp.DestTable
			if baseInfo, isDeferred := m.baseTableEngines[destTableKey]; isDeferred {
				// Case 1: The MV target was deferred - create it now as SharedMergeTree
				m.logger.Info("Standalone mode: creating deferred table for MV target",
					zap.String("dest_table", destTableKey),
					zap.String("mv", mvOp.ViewName))
				createSQL := m.generateStandaloneTableSQL(baseInfo.Database, baseInfo.Table, baseInfo.Columns, baseInfo.Indexes, baseInfo.Projections, baseInfo.Engine)
				if err := m.conn.Exec(ctx, createSQL); err != nil {
					m.logger.Error("Standalone mode: failed to create MV target table",
						zap.String("dest_table", destTableKey),
						zap.Error(err))
					return err
				}
				// Remove from deferred list since we created it
				delete(m.baseTableEngines, destTableKey)
			} else if distributedTableKey, isView := m.baseToDistributed[destTableKey]; isView {
				// Case 2: The MV target is a VIEW - redirect to the distributed_ table
				// MVs cannot write TO VIEWs, they need to write to real tables
				m.logger.Info("Standalone mode: redirecting MV target from VIEW to distributed table",
					zap.String("original_dest", destTableKey),
					zap.String("redirected_dest", distributedTableKey),
					zap.String("mv", mvOp.ViewName))
				// Parse the distributed table key to get database and table
				parts := strings.SplitN(distributedTableKey, ".", 2)
				if len(parts) == 2 {
					mvOp.DestTable = parts[1]
					// Update sql to use the redirected MV
					sql = mvOp.ToSQL()
				}
			}
		}
	}

	m.logger.Info("Waiting for running mutations before running the operation")
	if err := m.WaitForRunningMutations(ctx); err != nil {
		return err
	}

	m.logger.Info("Waiting for distributed DDL queue before running the operation")
	if err := m.WaitDistributedDDLQueue(ctx); err != nil {
		return err
	}

	if shouldWaitForDistributionQueue, database, table := operation.ShouldWaitForDistributionQueue(); shouldWaitForDistributionQueue {
		m.logger.Info("Waiting for distribution queue", zap.String("database", database), zap.String("table", table))
		if err := m.WaitForDistributionQueue(ctx, database, table); err != nil {
			return err
		}
	}

	// If sql wasn't set by standalone mode handling, generate it normally
	if sql == "" {
		sql = operation.ToSQL()
	}

	// Skip operations that are deferred in standalone mode (base tables waiting for distributed)
	if sql == "-- STANDALONE_SKIP_BASE_TABLE" {
		m.logger.Info("Standalone mode: skipping base table creation (will be created as VIEW later)")
		return m.InsertMigrationEntry(ctx, database, migrationID, FinishedStatus)
	}

	m.logger.Info("Running operation", zap.String("sql", sql))
	err := m.conn.Exec(ctx, sql)
	if err != nil {
		return err
	}

	// In standalone mode, create pending VIEW for base tables after SharedMergeTree is created
	if m.clusterName == "" && len(m.pendingBaseTableViews) > 0 {
		for baseTableKey, distributedTableKey := range m.pendingBaseTableViews {
			// Parse base table key to get database and table
			parts := strings.SplitN(baseTableKey, ".", 2)
			if len(parts) != 2 {
				continue
			}
			baseDB, baseTable := parts[0], parts[1]

			// Special case: schema_migrations_v2 must be a real table (migrator uses ALTER TABLE on it)
			// Skip creating a VIEW for it - the actual table was already created
			if baseTable == "schema_migrations_v2" {
				m.logger.Info("Standalone mode: skipping VIEW creation for schema_migrations_v2 (needs to be real table)",
					zap.String("base_table", baseTableKey))
				delete(m.pendingBaseTableViews, baseTableKey)
				continue
			}

			viewSQL := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s",
				baseDB, baseTable, distributedTableKey)
			m.logger.Info("Standalone mode: creating VIEW for base table",
				zap.String("base_table", baseTableKey),
				zap.String("pointing_to", distributedTableKey),
				zap.String("sql", viewSQL))

			if viewErr := m.conn.Exec(ctx, viewSQL); viewErr != nil {
				m.logger.Error("Standalone mode: failed to create VIEW for base table",
					zap.String("base_table", baseTableKey),
					zap.Error(viewErr))
				return viewErr
			}

			// Mark as consumed - remove from both pending lists
			delete(m.pendingBaseTableViews, baseTableKey)
			delete(m.baseTableEngines, baseTableKey) // Also remove from engines so MV handling doesn't try to create it
		}
	}

	duration := time.Since(start)
	m.logger.Info("Operation completed", zap.Uint64("migration_id", migrationID), zap.String("database", database), zap.Duration("duration", duration))

	return m.InsertMigrationEntry(ctx, database, migrationID, FinishedStatus)
}

// generateStandaloneTableSQL generates CREATE TABLE SQL for standalone mode (SharedMergeTree)
// Compatible with both ObsessionDB and ClickHouse Cloud
func (m *MigrationManager) generateStandaloneTableSQL(database, table string, columns []Column, indexes []Index, projections []Projection, engine TableEngine) string {
	var sql strings.Builder
	sql.WriteString("CREATE TABLE IF NOT EXISTS ")
	sql.WriteString(database)
	sql.WriteString(".")
	sql.WriteString(table)

	// Columns - convert DEFAULT now() to MATERIALIZED now() for timestamp columns
	// This fixes batch insert mismatches where OTEL doesn't provide timestamp values
	columnParts := []string{}
	for _, column := range columns {
		// Check if this is a timestamp column with DEFAULT now() that should be MATERIALIZED
		if column.Name == "timestamp" && strings.Contains(strings.ToLower(column.Default), "now()") {
			m.logger.Info("Standalone mode: converting timestamp DEFAULT to MATERIALIZED",
				zap.String("table", database+"."+table),
				zap.String("column", column.Name),
				zap.String("default", column.Default))
			// Create a copy with MATERIALIZED instead of DEFAULT
			modifiedColumn := column
			modifiedColumn.Materialized = column.Default
			modifiedColumn.Default = ""
			columnParts = append(columnParts, modifiedColumn.ToSQL())
		} else {
			columnParts = append(columnParts, column.ToSQL())
		}
	}
	sql.WriteString(" (")
	sql.WriteString(strings.Join(columnParts, ", "))

	// Indexes
	for _, index := range indexes {
		sql.WriteString(", ")
		sql.WriteString(index.ToSQL())
	}

	// Projections
	for _, projection := range projections {
		sql.WriteString(", ")
		sql.WriteString(projection.ToSQL())
	}
	sql.WriteString(")")

	// Engine - convert to SharedMergeTree variant
	sql.WriteString(" ENGINE = ")
	sql.WriteString(m.engineToStandaloneSQL(engine))

	return sql.String()
}

// engineToStandaloneSQL converts a table engine to standalone mode SQL
// It uses Shared* variants (SharedMergeTree, SharedReplacingMergeTree, etc.)
func (m *MigrationManager) engineToStandaloneSQL(engine TableEngine) string {
	var sql strings.Builder

	switch e := engine.(type) {
	case *MergeTree:
		sql.WriteString("SharedMergeTree")
		sql.WriteString(m.mergeTreeParamsToSQL(e, true))
	case MergeTree:
		sql.WriteString("SharedMergeTree")
		sql.WriteString(m.mergeTreeParamsToSQL(&e, true))
	case *ReplacingMergeTree:
		sql.WriteString("SharedReplacingMergeTree")
		sql.WriteString(m.mergeTreeParamsToSQL(&e.MergeTree, true))
	case ReplacingMergeTree:
		sql.WriteString("SharedReplacingMergeTree")
		sql.WriteString(m.mergeTreeParamsToSQL(&e.MergeTree, true))
	case *AggregatingMergeTree:
		sql.WriteString("SharedAggregatingMergeTree")
		sql.WriteString(m.mergeTreeParamsToSQL(&e.MergeTree, true))
	case AggregatingMergeTree:
		sql.WriteString("SharedAggregatingMergeTree")
		sql.WriteString(m.mergeTreeParamsToSQL(&e.MergeTree, true))
	case *SummingMergeTree:
		sql.WriteString("SharedSummingMergeTree")
		sql.WriteString(m.mergeTreeParamsToSQL(&e.MergeTree, true))
	case SummingMergeTree:
		sql.WriteString("SharedSummingMergeTree")
		sql.WriteString(m.mergeTreeParamsToSQL(&e.MergeTree, true))
	default:
		// Fallback: use the engine as-is
		m.logger.Warn("Unknown engine type, using as-is", zap.String("engine", engine.ToSQL()))
		sql.WriteString(engine.ToSQL())
	}

	return sql.String()
}

// mergeTreeParamsToSQL generates the params part of MergeTree engine
// The addStoragePolicy parameter is kept for API compatibility but no longer used
func (m *MigrationManager) mergeTreeParamsToSQL(mt *MergeTree, addStoragePolicy bool) string {
	var sql strings.Builder

	if mt.PrimaryKey != "" {
		sql.WriteString(" PRIMARY KEY ")
		sql.WriteString(mt.PrimaryKey)
	}
	if mt.OrderBy != "" {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(mt.OrderBy)
	} else {
		// SharedMergeTree requires ORDER BY
		sql.WriteString(" ORDER BY tuple()")
	}
	if mt.PartitionBy != "" {
		sql.WriteString(" PARTITION BY ")
		sql.WriteString(mt.PartitionBy)
	}
	if mt.SampleBy != "" {
		sql.WriteString(" SAMPLE BY ")
		sql.WriteString(mt.SampleBy)
	}
	if mt.TTL != "" {
		sql.WriteString(" TTL ")
		sql.WriteString(mt.TTL)
	}

	// Settings - no longer add storage_policy automatically
	// This allows compatibility with both ObsessionDB and ClickHouse Cloud
	settings := mt.Settings

	if len(settings) > 0 {
		sql.WriteString(" SETTINGS ")
		sql.WriteString(settings.ToSQL())
	}

	return sql.String()
}

func (m *MigrationManager) InsertMigrationEntry(ctx context.Context, db string, migrationID uint64, status string) error {
	// In standalone mode, use local table instead of distributed table
	migrationsTable := "distributed_schema_migrations_v2"
	if m.clusterName == "" {
		migrationsTable = "schema_migrations_v2"
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (migration_id, status, created_at) VALUES (%d, '%s', '%s')", db, migrationsTable, migrationID, status, time.Now().UTC().Format("2006-01-02 15:04:05"))
	m.logger.Info("Inserting migration entry", zap.String("query", query))
	return m.conn.Exec(ctx, query)
}

func (m *MigrationManager) CheckMigrationStatus(ctx context.Context, db string, migrationID uint64, status string) (bool, error) {
	// In standalone mode, use local table instead of distributed table
	migrationsTable := "distributed_schema_migrations_v2"
	if m.clusterName == "" {
		migrationsTable = "schema_migrations_v2"
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE migration_id = %d SETTINGS final = 1;", db, migrationsTable, migrationID)
	m.logger.Info("Checking migration status", zap.String("query", query))

	var migrationSchemaMigrationRecord MigrationSchemaMigrationRecord
	if err := m.conn.QueryRow(ctx, query).ScanStruct(&migrationSchemaMigrationRecord); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}

		return false, err
	}

	return migrationSchemaMigrationRecord.Status == status, nil
}

func (m *MigrationManager) Close() error {
	return m.conn.Close()
}
