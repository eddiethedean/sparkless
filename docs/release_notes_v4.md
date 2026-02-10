# Sparkless v4.0.0 — Release notes

Sparkless v4 is a **Robin-only** release: the only supported execution backend is **Robin** (robin-sparkless). Polars, memory, file, and DuckDB backends have been removed. This is a breaking change for any code that relied on non-Robin backends.

**Migration:** See [migration_v3_to_v4.md](migration_v3_to_v4.md) for dependency change (robin-sparkless required), configuration (only `robin` valid), and behavioral differences (schema inference, unsupported expressions, type strictness).

**Known limitations:** See [v4_behavior_changes_and_known_differences.md](v4_behavior_changes_and_known_differences.md) for supported vs unsupported expressions (Phase 7: cast, substring, getItem supported; CaseWhen, window in select documented as gaps) and Robin semantics.

**Full changelog:** [CHANGELOG.md](../CHANGELOG.md) § 4.0.0.
