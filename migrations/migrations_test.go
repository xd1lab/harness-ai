package migrations_test

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/boltrope/boltrope/migrations"
)

// TestEmbedNonEmpty asserts the //go:embed actually captured the .sql files so a
// build that loses them fails loudly rather than applying an empty migration set.
func TestEmbedNonEmpty(t *testing.T) {
	t.Parallel()
	entries, err := fs.ReadDir(migrations.Source(), ".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var ups int
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".up.sql") {
			ups++
		}
	}
	if ups == 0 {
		t.Fatal("no *.up.sql migrations embedded; //go:embed lost the files")
	}
}

// TestUpMigrationsParseAsVersions asserts each up-migration filename starts with
// a zero-padded numeric version followed by '_', the form the iofs source driver
// requires; a malformed name would make migrate skip or reject the file.
func TestUpMigrationsParseAsVersions(t *testing.T) {
	t.Parallel()
	entries, err := fs.ReadDir(migrations.Source(), ".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		ver, _, ok := strings.Cut(name, "_")
		if !ok {
			t.Errorf("migration %q is not in <version>_<name>.sql form", name)
			continue
		}
		if ver == "" {
			t.Errorf("migration %q has an empty version prefix", name)
		}
		for _, r := range ver {
			if r < '0' || r > '9' {
				t.Errorf("migration %q version prefix %q is not numeric", name, ver)
				break
			}
		}
	}
}

// TestForwardOnly_RealMigrationsPass asserts the committed migration set has no
// destructive down on a protected log table — the convention CI enforces
// (ADR-0011 §6.1).
func TestForwardOnly_RealMigrationsPass(t *testing.T) {
	t.Parallel()
	if err := migrations.CheckForwardOnly(migrations.Source()); err != nil {
		t.Fatalf("committed migrations violate forward-only: %v", err)
	}
}

// TestForwardOnly_RejectsDestructiveDown drives the guard with synthetic
// down-migrations to prove it actually fires (a positive control), covering
// DROP/TRUNCATE/DELETE variants and confirming a comment-only mention or a
// non-protected table does not trip it.
func TestForwardOnly_RejectsDestructiveDown(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		file    string
		content string
		wantErr bool
	}{
		{
			name:    "drop events",
			file:    "0009_x.down.sql",
			content: "DROP TABLE events;",
			wantErr: true,
		},
		{
			name:    "drop if exists sessions",
			file:    "0009_x.down.sql",
			content: "DROP TABLE IF EXISTS sessions;",
			wantErr: true,
		},
		{
			name:    "truncate events",
			file:    "0009_x.down.sql",
			content: "TRUNCATE TABLE events;",
			wantErr: true,
		},
		{
			name:    "delete from sessions",
			file:    "0009_x.down.sql",
			content: "DELETE FROM sessions WHERE true;",
			wantErr: true,
		},
		{
			name:    "schema-qualified drop",
			file:    "0009_x.down.sql",
			content: "DROP TABLE public.events;",
			wantErr: true,
		},
		{
			name:    "drop a non-protected table is allowed",
			file:    "0009_x.down.sql",
			content: "DROP TABLE some_read_side_projection;",
			wantErr: false,
		},
		{
			name:    "events only in a comment is allowed",
			file:    "0009_x.down.sql",
			content: "-- this does not DROP TABLE events\nSELECT 1;",
			wantErr: false,
		},
		{
			name:    "up file is never scanned",
			file:    "0009_x.up.sql",
			content: "DROP TABLE events;",
			wantErr: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src := fstest.MapFS{tc.file: {Data: []byte(tc.content)}}
			err := migrations.CheckForwardOnly(src)
			if tc.wantErr && err == nil {
				t.Fatalf("expected forward-only violation for %q, got nil", tc.content)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected forward-only violation for %q: %v", tc.content, err)
			}
		})
	}
}
