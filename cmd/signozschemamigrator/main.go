package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	schema_migrator "github.com/SigNoz/signoz-otel-collector/cmd/signozschemamigrator/schema_migrator"
	"github.com/SigNoz/signoz-otel-collector/constants"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func getLogger() *zap.Logger {
	// Always verbose logging for schema migrator
	config := zap.NewDevelopmentConfig()
	config.Encoding = "json"
	config.EncoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, err := config.Build()
	if err != nil {
		log.Fatalf("Failed to initialize zap logger %v", err)
	}
	return logger
}

func main() {
	cmd := &cobra.Command{
		Use:   "signoz-schema-migrator",
		Short: "SigNoz Schema Migrator for ObsessionDB/SharedMergeTree",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			v := viper.New()

			v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
			v.AutomaticEnv()

			cmd.Flags().VisitAll(func(f *pflag.Flag) {
				configName := f.Name
				if !f.Changed && v.IsSet(configName) {
					val := v.Get(configName)
					err := cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val))
					if err != nil {
						panic(err)
					}
				}
			})
		},
	}

	var dsn string
	var development bool
	var mvGate string

	cmd.PersistentFlags().StringVar(&dsn, "dsn", "", "Clickhouse DSN")
	cmd.PersistentFlags().BoolVar(&development, "dev", false, "Development mode")
	cmd.PersistentFlags().StringVar(&mvGate, "mv-gate", "fail",
		"What a materialized view left misconfigured does to sync/async: fail, or warn (break-glass only)")

	registerSyncMigrate(cmd)
	registerAsyncMigrate(cmd)
	registerCheckMVs(cmd)

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func registerSyncMigrate(cmd *cobra.Command) {

	var upVersions string
	var downVersions string

	syncCmd := &cobra.Command{
		Use:   "sync",
		Short: "Run migrations in sync mode",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := getLogger()

			dsn := cmd.Flags().Lookup("dsn").Value.String()
			development := strings.ToLower(cmd.Flags().Lookup("dev").Value.String()) == "true"
			warnOnly, err := mvGateWarnOnly(cmd)
			if err != nil {
				return err
			}

			logger.Info("Running migrations in sync mode (standalone/SharedMergeTree)",
				zap.String("dsn", dsn),
				zap.Bool("enable-logs-migrations-v2", constants.EnableLogsMigrationsV2))

			upVersions := []uint64{}
			for _, version := range strings.Split(cmd.Flags().Lookup("up").Value.String(), ",") {
				if version == "" {
					continue
				}
				v, err := strconv.ParseUint(version, 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse version: %w", err)
				}
				upVersions = append(upVersions, v)
			}
			logger.Info("Up migrations", zap.Any("versions", upVersions))

			downVersions := []uint64{}
			for _, version := range strings.Split(cmd.Flags().Lookup("down").Value.String(), ",") {
				if version == "" {
					continue
				}
				v, err := strconv.ParseUint(version, 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse version: %w", err)
				}
				downVersions = append(downVersions, v)
			}
			logger.Info("Down migrations", zap.Any("versions", downVersions))

			if len(upVersions) != 0 && len(downVersions) != 0 {
				return fmt.Errorf("cannot provide both up and down migrations")
			}

			opts, err := clickhouse.ParseDSN(dsn)
			if err != nil {
				return fmt.Errorf("failed to parse dsn: %w", err)
			}
			logger.Info("Parsed DSN", zap.Any("opts", opts))

			conn, err := clickhouse.Open(opts)
			if err != nil {
				return fmt.Errorf("failed to open connection: %w", err)
			}
			logger.Info("Opened connection")

			// Standalone mode: no cluster, no replication
			manager, err := schema_migrator.NewMigrationManager(
				schema_migrator.WithClusterName(""), // Empty = standalone mode
				schema_migrator.WithReplicationEnabled(false),
				schema_migrator.WithConn(conn),
				schema_migrator.WithConnOptions(*opts),
				schema_migrator.WithLogger(logger),
				schema_migrator.WithDevelopment(development),
				schema_migrator.WithMVGateWarnOnly(warnOnly),
			)
			if err != nil {
				return fmt.Errorf("failed to create migration manager: %w", err)
			}
			err = manager.Bootstrap()
			if err != nil {
				return fmt.Errorf("failed to bootstrap migrations: %w", err)
			}
			logger.Info("Bootstrapped migrations")

			err = manager.RunSquashedMigrations(context.Background())
			if err != nil {
				return fmt.Errorf("failed to run squashed migrations: %w", err)
			}
			logger.Info("Ran squashed migrations")

			if len(downVersions) != 0 {
				logger.Info("Migrating down")
				if err := manager.MigrateDownSync(context.Background(), downVersions); err != nil {
					return err
				}
			} else {
				logger.Info("Migrating up")
				if err := manager.MigrateUpSync(context.Background(), upVersions); err != nil {
					return err
				}
			}

			// An explicit --up/--down is a targeted (often rollback) run: the
			// catalog is intentionally not at the state every migration leaves.
			if len(upVersions) != 0 || len(downVersions) != 0 {
				logger.Info("Skipping materialized view repair for an explicit --up/--down run")
				return nil
			}
			logger.Info("Running post-migration MV repair for standalone mode")
			return manager.RecreateMaterializedViewsForStandalone(context.Background())
		},
	}

	syncCmd.Flags().StringVar(&upVersions, "up", "", "Up migrations to run, comma separated. Leave empty to run all up migrations")
	syncCmd.Flags().StringVar(&downVersions, "down", "", "Down migrations to run, comma separated. Must provide down migrations explicitly to run")

	cmd.AddCommand(syncCmd)
}

func registerAsyncMigrate(cmd *cobra.Command) {

	var upVersions string
	var downVersions string

	asyncCmd := &cobra.Command{
		Use:   "async",
		Short: "Run migrations in async mode",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := getLogger()

			dsn := cmd.Flags().Lookup("dsn").Value.String()
			development := strings.ToLower(cmd.Flags().Lookup("dev").Value.String()) == "true"
			warnOnly, err := mvGateWarnOnly(cmd)
			if err != nil {
				return err
			}

			logger.Info("Running migrations in async mode (standalone/SharedMergeTree)",
				zap.String("dsn", dsn),
				zap.Bool("enable-logs-migrations-v2", constants.EnableLogsMigrationsV2))

			upVersions := []uint64{}
			for _, version := range strings.Split(cmd.Flags().Lookup("up").Value.String(), ",") {
				if version == "" {
					continue
				}
				v, err := strconv.ParseUint(version, 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse version: %w", err)
				}
				upVersions = append(upVersions, v)
			}
			logger.Info("Up migrations", zap.Any("versions", upVersions))

			downVersions := []uint64{}
			for _, version := range strings.Split(cmd.Flags().Lookup("down").Value.String(), ",") {
				if version == "" {
					continue
				}
				v, err := strconv.ParseUint(version, 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse version: %w", err)
				}
				downVersions = append(downVersions, v)
			}
			logger.Info("Down migrations", zap.Any("versions", downVersions))

			if len(upVersions) != 0 && len(downVersions) != 0 {
				return fmt.Errorf("cannot provide both up and down migrations")
			}

			opts, err := clickhouse.ParseDSN(dsn)
			if err != nil {
				return fmt.Errorf("failed to parse dsn: %w", err)
			}
			logger.Info("Parsed DSN", zap.Any("opts", opts))

			conn, err := clickhouse.Open(opts)
			if err != nil {
				return fmt.Errorf("failed to open connection: %w", err)
			}
			logger.Info("Opened connection")

			// Standalone mode: no cluster, no replication
			manager, err := schema_migrator.NewMigrationManager(
				schema_migrator.WithClusterName(""), // Empty = standalone mode
				schema_migrator.WithReplicationEnabled(false),
				schema_migrator.WithConn(conn),
				schema_migrator.WithConnOptions(*opts),
				schema_migrator.WithLogger(logger),
				schema_migrator.WithDevelopment(development),
				schema_migrator.WithMVGateWarnOnly(warnOnly),
			)
			if err != nil {
				return fmt.Errorf("failed to create migration manager: %w", err)
			}

			if len(downVersions) != 0 {
				logger.Info("Migrating down")
				if err := manager.MigrateDownAsync(context.Background(), downVersions); err != nil {
					return err
				}
			} else {
				logger.Info("Migrating up")
				if err := manager.MigrateUpAsync(context.Background(), upVersions); err != nil {
					return err
				}
			}

			// An explicit --up/--down is a targeted (often rollback) run: the
			// catalog is intentionally not at the state every migration leaves.
			if len(upVersions) != 0 || len(downVersions) != 0 {
				logger.Info("Skipping materialized view repair for an explicit --up/--down run")
				return nil
			}
			logger.Info("Running post-migration MV repair for standalone mode")
			return manager.RecreateMaterializedViewsForStandalone(context.Background())
		},
	}

	asyncCmd.Flags().StringVar(&upVersions, "up", "", "Up migrations to run, comma separated. Leave empty to run all up migrations")
	asyncCmd.Flags().StringVar(&downVersions, "down", "", "Down migrations to run, comma separated. Must provide down migrations explicitly to run")

	cmd.AddCommand(asyncCmd)
}

// registerCheckMVs adds the read-only preflight for the materialized view
// repair that sync and async run at the end. It prints the exact actions they
// would take and exits non-zero if anything would still make them fail, so it
// can be run against production before rolling out a new migrator image.
func registerCheckMVs(cmd *cobra.Command) {
	checkCmd := &cobra.Command{
		Use:          "check-mvs",
		Short:        "Report the materialized view repairs sync would make, without changing anything",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := getLogger()

			// The DSN carries the password, so it is never logged here.
			opts, err := clickhouse.ParseDSN(cmd.Flags().Lookup("dsn").Value.String())
			if err != nil {
				return fmt.Errorf("failed to parse dsn: %w", err)
			}
			conn, err := clickhouse.Open(opts)
			if err != nil {
				return fmt.Errorf("failed to open connection: %w", err)
			}
			defer conn.Close()

			manager, err := schema_migrator.NewMigrationManager(
				schema_migrator.WithClusterName(""), // standalone mode
				schema_migrator.WithReplicationEnabled(false),
				schema_migrator.WithConn(conn),
				schema_migrator.WithConnOptions(*opts),
				schema_migrator.WithLogger(logger),
			)
			if err != nil {
				return fmt.Errorf("failed to create migration manager: %w", err)
			}

			report, err := manager.CheckMaterializedViewsForStandalone(context.Background())
			if err != nil {
				return err
			}
			if len(report.Actions) == 0 {
				fmt.Println("no materialized view changes needed")
			}
			for _, action := range report.Actions {
				fmt.Println("would run:", action)
			}
			for _, problem := range report.Problems {
				fmt.Println("would fail:", problem)
			}
			if len(report.Problems) > 0 {
				return fmt.Errorf("%d problem(s) would make sync fail", len(report.Problems))
			}
			fmt.Println("ok: sync would leave every materialized view consistent")
			return nil
		},
	}

	cmd.AddCommand(checkCmd)
}

func mvGateWarnOnly(cmd *cobra.Command) (bool, error) {
	switch mode := cmd.Flags().Lookup("mv-gate").Value.String(); mode {
	case "fail":
		return false, nil
	case "warn":
		return true, nil
	default:
		return false, fmt.Errorf("invalid --mv-gate %q: want fail or warn", mode)
	}
}
