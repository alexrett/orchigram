// Package version contains release metadata injected at build time.
package version

var (
	// Version is the semantic release version.
	Version = "dev"
	// Commit is the source revision used for the build.
	Commit = "unknown"
	// Date is the reproducible build timestamp.
	Date = "unknown"
)

// String returns a human-readable build identity.
func String() string {
	return Version + " (commit " + Commit + ", built " + Date + ")"
}
