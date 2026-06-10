package migrations

import (
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"
)

// protectedLogTables are the append-only single-source-of-truth tables that a
// down-migration must never drop or truncate (ADR-0011 §"Migration policy";
// architecture §6.1). Destroying them is unrecoverable data loss.
var protectedLogTables = []string{"events", "sessions"}

// destructiveStmt matches a DROP TABLE / TRUNCATE / DELETE FROM statement
// targeting a table name, case-insensitively. It is intentionally broad: any
// such statement naming a protected table in a down-migration is rejected. The
// optional "IF EXISTS" and "ONLY" qualifiers, an optional schema prefix, and an
// optional opening double-quote are tolerated (Go's RE2 has no backreference, so
// a closing quote is not matched — the captured identifier is enough). The
// captured table name is submatch index [destructiveTableGroup].
var destructiveStmt = regexp.MustCompile(
	`(?is)\b(?:drop\s+table|truncate(?:\s+table)?|delete\s+from)\b\s+(?:if\s+exists\s+)?(?:only\s+)?(?:[a-z_][a-z0-9_]*\.)?"?([a-z_][a-z0-9_]*)`,
)

// destructiveTableGroup is the submatch index of the captured table name in
// [destructiveStmt].
const destructiveTableGroup = 1

// CheckForwardOnly scans every *.down.sql migration in src and returns a non-nil
// error if any of them drops, truncates, or deletes from a protected log table
// ([protectedLogTables]). It is the mechanical enforcement of the forward-only
// convention (architecture §6.1): the package test and CI call it so a
// destructive down on events/sessions fails the build rather than slipping past
// review. A non-protected table's down statement (e.g. a future read-side
// projection table) is permitted.
//
// It is a pure function over the file set (no DB), so it runs in unit tests.
func CheckForwardOnly(src fs.FS) error {
	entries, err := fs.ReadDir(src, ".")
	if err != nil {
		return fmt.Errorf("migrations: reading source dir: %w", err)
	}

	protected := make(map[string]struct{}, len(protectedLogTables))
	for _, t := range protectedLogTables {
		protected[t] = struct{}{}
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".down.sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	var violations []string
	for _, name := range names {
		data, readErr := fs.ReadFile(src, name)
		if readErr != nil {
			return fmt.Errorf("migrations: reading %s: %w", name, readErr)
		}
		for _, m := range destructiveStmt.FindAllStringSubmatch(stripSQLComments(string(data)), -1) {
			table := strings.ToLower(m[destructiveTableGroup])
			if _, isProtected := protected[table]; isProtected {
				violations = append(violations,
					fmt.Sprintf("%s: destructive statement on protected log table %q", name, table))
			}
		}
	}

	if len(violations) > 0 {
		return fmt.Errorf(
			"migrations: forward-only convention violated (destructive down-migration on the event log; ADR-0011 §6.1):\n  %s",
			strings.Join(violations, "\n  "))
	}
	return nil
}

// stripSQLComments removes "--" line comments and /* */ block comments so the
// destructive-statement scan does not match a table name that only appears in a
// comment. It is a deliberately simple lexer adequate for migration SQL (it does
// not need to handle dollar-quoted strings for the comment case).
func stripSQLComments(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))
	for i := 0; i < len(sql); i++ {
		// Block comment.
		if i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*' {
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				break
			}
			i += end + 3
			b.WriteByte(' ')
			continue
		}
		// Line comment.
		if i+1 < len(sql) && sql[i] == '-' && sql[i+1] == '-' {
			nl := strings.IndexByte(sql[i:], '\n')
			if nl < 0 {
				break
			}
			i += nl
			b.WriteByte('\n')
			continue
		}
		b.WriteByte(sql[i])
	}
	return b.String()
}
