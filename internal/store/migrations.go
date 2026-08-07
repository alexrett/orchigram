// Package store contains the authoritative SQLite persistence layer.
package store

import "embed"

// Migrations contains the ordered, append-only SQLite schema migrations.
//
//go:embed migrations/*.sql
var Migrations embed.FS
