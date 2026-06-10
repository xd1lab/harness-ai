// Package migrations holds the Boltrope event-store schema as embedded
// golang-migrate SQL files and exposes them as an [io/fs.FS] so the migrate
// runner ([github.com/xd1lab/harness-ai/internal/orchestrator/infra/db]) can
// apply them to a DSN without any filesystem dependency at runtime (ADR-0011
// §"Migration policy"; architecture §6.1, §10.2).
//
// # Forward-only convention (CI-checked)
//
// Migrations are expand/contract and FORWARD-ONLY for the log tables
// (events/sessions): there are deliberately NO down-migration files that drop
// or truncate events/sessions, because a destructive down on the single source
// of truth is a data-loss anti-pattern (ADR-0011; architecture §6.1). This is
// not merely a comment: [CheckForwardOnly] mechanically scans the embedded set
// and rejects any *.down.sql that drops/truncates a protected log table, and
// the package test asserts it — so the convention fails CI rather than relying
// on review.
//
// # Naming
//
// Files follow golang-migrate's "<version>_<name>.<up|down>.sql" convention with
// a zero-padded numeric version. The iofs source driver parses these names.
package migrations

import (
	"embed"
	"io/fs"
)

// FS embeds every *.sql migration file in this directory. It is consumed via
// [Source], which adapts it to the golang-migrate iofs source driver.
//
//go:embed *.sql
var FS embed.FS

// Source returns the embedded migrations as an [io/fs.FS] rooted at this
// package directory, suitable for golang-migrate's iofs source driver
// (iofs.New(migrations.Source(), ".")). It returns the embedded [FS] directly;
// the *.sql files live at the FS root.
func Source() fs.FS { return FS }
