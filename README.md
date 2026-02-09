# SigNoz OpenTelemetry Collector

Fork of [SigNoz/signoz-otel-collector](https://github.com/SigNoz/signoz-otel-collector).

## Fork Changes

We patched the **schema-migrator** to support SharedMergeTree engine of [ObsessionDB](https://obsessiondb.com) and ClickHouse Cloud.

See [cmd/signozschemamigrator](cmd/signozschemamigrator) for details.

For the full setup, see [obsessiondb/signoz-obsessiondb](https://github.com/obsessiondb/signoz-obsessiondb).
