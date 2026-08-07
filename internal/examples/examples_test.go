package examples

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
)

func TestShippedResourcesAreStrictAndFlowsCompile(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "**", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	rootPaths, err := filepath.Glob(filepath.Join("..", "..", "examples", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, rootPaths...)
	if len(paths) == 0 {
		t.Fatal("no example resources found")
	}
	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, readErr := os.ReadFile(path) //nolint:gosec // Test reads repository-owned example paths.
			if readErr != nil {
				t.Fatal(readErr)
			}
			if strings.Contains(string(data), "https://") && strings.Contains(path, "teams") {
				t.Fatal("Teams example must not contain a webhook URL")
			}
			document, decodeErr := resource.DecodeStrict(data)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if document.Kind == "Flow" {
				flowResource, decodeFlowErr := resource.DecodeFlow(document.JSON)
				if decodeFlowErr != nil {
					t.Fatal(decodeFlowErr)
				}
				_, diagnostics := flow.NewCompiler(nil).Compile(flowResource)
				if len(diagnostics) != 0 {
					t.Fatalf("compile diagnostics: %+v", diagnostics)
				}
			}
		})
	}
}
