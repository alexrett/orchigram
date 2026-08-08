package releasepack

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

const spdxNamespacePrefix = "https://orchigram.dev/spdx/"

// NormalizeSPDX replaces Syft's invocation-specific SPDX identity with values
// derived from the immutable release artifact and release commit timestamp.
func NormalizeSPDX(document, artifact []byte, created string) ([]byte, error) {
	timestamp, err := time.Parse(time.RFC3339, created)
	if err != nil {
		return nil, fmt.Errorf("parse SPDX creation time: %w", err)
	}
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode SPDX JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return nil, errors.New("decode SPDX JSON: multiple JSON values")
	} else if !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode SPDX JSON trailing data: %w", err)
	}
	if value["spdxVersion"] != "SPDX-2.3" {
		return nil, fmt.Errorf("expected SPDX-2.3 document, got %v", value["spdxVersion"])
	}
	creationInfo, ok := value["creationInfo"].(map[string]any)
	if !ok {
		return nil, errors.New("SPDX document has no creationInfo object")
	}
	digest := sha256.Sum256(artifact)
	value["documentNamespace"] = spdxNamespacePrefix + hex.EncodeToString(digest[:])
	creationInfo["created"] = timestamp.UTC().Format(time.RFC3339)

	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode normalized SPDX JSON: %w", err)
	}
	return append(normalized, '\n'), nil
}

// NormalizeSPDXFile atomically normalizes an SPDX document against its artifact.
func NormalizeSPDXFile(documentPath, artifactPath, created string) error {
	if documentPath == "" || artifactPath == "" || created == "" {
		return errors.New("file, artifact and date are required")
	}
	document, err := os.ReadFile(documentPath) //nolint:gosec // Release-only paths are supplied by the trusted build configuration.
	if err != nil {
		return err
	}
	artifact, err := os.ReadFile(artifactPath) //nolint:gosec // Release-only paths are supplied by the trusted build configuration.
	if err != nil {
		return err
	}
	normalized, err := NormalizeSPDX(document, artifact, created)
	if err != nil {
		return err
	}
	return writeAtomic(documentPath, normalized)
}
