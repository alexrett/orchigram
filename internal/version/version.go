// Package version contains release metadata injected at build time.
package version

import "github.com/Masterminds/semver/v3"

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

// Semver returns the immutable plugin bundle version for this build.
func Semver() string {
	if _, err := semver.StrictNewVersion(Version); err == nil {
		return Version
	}
	return "0.0.0-dev"
}
