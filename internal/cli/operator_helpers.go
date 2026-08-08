package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	"github.com/alexrett/orchigram/internal/resource"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

type manifestDocument struct {
	data                    []byte
	expectedResourceVersion uint64
}

func readManifestDocuments(reader io.Reader) ([]manifestDocument, error) {
	decoder := yaml.NewDecoder(reader)
	var documents []manifestDocument
	for index := 1; ; index++ {
		var node yaml.Node
		if err := decoder.Decode(&node); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode document %d: %w", index, err)
		}
		if emptyYAMLDocument(&node) {
			continue
		}
		data, err := yaml.Marshal(&node)
		if err != nil {
			return nil, fmt.Errorf("encode document %d: %w", index, err)
		}
		var envelope struct {
			Metadata struct {
				ResourceVersion uint64 `yaml:"resourceVersion"`
			} `yaml:"metadata"`
		}
		if err := yaml.Unmarshal(data, &envelope); err != nil {
			return nil, fmt.Errorf("inspect document %d metadata: %w", index, err)
		}
		documents = append(documents, manifestDocument{data: data, expectedResourceVersion: envelope.Metadata.ResourceVersion})
	}
	if len(documents) == 0 {
		return nil, errors.New("manifest contains no resources")
	}
	return documents, nil
}

func emptyYAMLDocument(node *yaml.Node) bool {
	if node == nil || len(node.Content) == 0 {
		return true
	}
	root := node.Content[0]
	return root.Kind == yaml.ScalarNode && (root.Tag == "!!null" || strings.TrimSpace(root.Value) == "")
}

func clearYAMLStyles(node *yaml.Node) {
	if node == nil {
		return
	}
	node.Style = 0
	for _, child := range node.Content {
		clearYAMLStyles(child)
	}
}

func parseLabelSelectors(values []string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	labels := make(map[string]string, len(values))
	for _, value := range values {
		key, selected, ok := strings.Cut(value, "=")
		key, selected = strings.TrimSpace(key), strings.TrimSpace(selected)
		if !ok || key == "" || selected == "" {
			return nil, fmt.Errorf("label selector %q must be key=value", value)
		}
		if previous, exists := labels[key]; exists && previous != selected {
			return nil, fmt.Errorf("label selector %q conflicts with %s=%s", value, key, previous)
		}
		labels[key] = selected
	}
	return labels, nil
}

func printDiagnostics(writer io.Writer, diagnostics []*controlv1alpha1.Diagnostic) (bool, error) {
	hasErrors := false
	for _, diagnostic := range diagnostics {
		hasErrors = hasErrors || diagnostic.GetSeverity() == controlv1alpha1.Diagnostic_SEVERITY_ERROR || diagnostic.GetSeverity() == controlv1alpha1.Diagnostic_SEVERITY_UNSPECIFIED
		if _, err := fmt.Fprintf(writer, "%s: %s (%s)\n", diagnostic.GetPath(), diagnostic.GetMessage(), diagnostic.GetCode()); err != nil {
			return false, err
		}
	}
	return hasErrors, nil
}

func printDocuments(writer io.Writer, documents [][]byte, format string) error {
	if len(documents) == 1 {
		return printDocument(writer, documents[0], format)
	}
	if format == "json" {
		var output bytes.Buffer
		output.WriteByte('[')
		for index, document := range documents {
			if !json.Valid(document) {
				return fmt.Errorf("document %d is not valid JSON", index+1)
			}
			if index > 0 {
				output.WriteByte(',')
			}
			output.Write(document)
		}
		output.WriteByte(']')
		return printDocument(writer, output.Bytes(), format)
	}
	if format != "yaml" {
		return fmt.Errorf("unsupported output format %q", format)
	}
	for index, document := range documents {
		if index > 0 {
			if _, err := io.WriteString(writer, "---\n"); err != nil {
				return err
			}
		}
		if err := printDocument(writer, document, format); err != nil {
			return err
		}
	}
	return nil
}

func renderGraph(writer io.Writer, response *controlv1alpha1.PreviewGraphResponse) error {
	nodes := append([]*controlv1alpha1.GraphNode(nil), response.GetNodes()...)
	edges := append([]*controlv1alpha1.GraphEdge(nil), response.GetEdges()...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].GetId() < nodes[j].GetId() })
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].GetFrom() != edges[j].GetFrom() {
			return edges[i].GetFrom() < edges[j].GetFrom()
		}
		if edges[i].GetTo() != edges[j].GetTo() {
			return edges[i].GetTo() < edges[j].GetTo()
		}
		return edges[i].GetCondition() < edges[j].GetCondition()
	})
	connected := make(map[string]bool, len(nodes))
	for _, edge := range edges {
		connected[edge.GetFrom()] = true
		connected[edge.GetTo()] = true
		condition := ""
		if edge.GetCondition() != "" {
			condition = " --(" + edge.GetCondition() + ")"
		}
		if _, err := fmt.Fprintf(writer, "[%s]%s--> [%s]\n", edge.GetFrom(), condition, edge.GetTo()); err != nil {
			return err
		}
	}
	for _, node := range nodes {
		if connected[node.GetId()] {
			continue
		}
		if _, err := fmt.Fprintf(writer, "[%s]\n", node.GetId()); err != nil {
			return err
		}
	}
	if len(nodes) > 0 {
		if _, err := io.WriteString(writer, "\nNODES\n"); err != nil {
			return err
		}
	}
	for _, node := range nodes {
		label := node.GetLabel()
		if label == "" {
			label = node.GetId()
		}
		if _, err := fmt.Fprintf(writer, "%s\t%s\t%s\n", node.GetId(), label, node.GetAction()); err != nil {
			return err
		}
	}
	return nil
}

func printRunDescription(
	writer io.Writer,
	run *controlv1alpha1.RunSummary,
	plan *controlv1alpha1.CompileResponse,
	attempts *controlv1alpha1.ListAttemptsResponse,
	artifacts *controlv1alpha1.ListArtifactsResponse,
	format string,
) error {
	type attemptProjection struct {
		NodeID           string `json:"nodeId"`
		LogicalIteration uint32 `json:"logicalIteration"`
		Attempt          uint32 `json:"attempt"`
		FrameworkAttempt uint32 `json:"frameworkAttempt"`
		Phase            string `json:"phase"`
		ExitOutcome      string `json:"exitOutcome,omitempty"`
		Error            string `json:"error,omitempty"`
		StartedAt        string `json:"startedAt,omitempty"`
		CompletedAt      string `json:"completedAt,omitempty"`
	}
	type artifactProjection struct {
		UID       string `json:"uid"`
		NodeID    string `json:"nodeId"`
		Attempt   uint32 `json:"attempt"`
		Name      string `json:"name"`
		MediaType string `json:"mediaType"`
		SizeBytes int64  `json:"sizeBytes"`
		SHA256    string `json:"sha256"`
	}
	type description struct {
		UID                string               `json:"uid"`
		Flow               string               `json:"flow"`
		Phase              string               `json:"phase"`
		PlanHash           string               `json:"planHash"`
		InterpreterVersion string               `json:"interpreterVersion"`
		CreatedAt          string               `json:"createdAt"`
		UpdatedAt          string               `json:"updatedAt"`
		ExecutionPlan      json.RawMessage      `json:"executionPlan"`
		Attempts           []attemptProjection  `json:"attempts"`
		Artifacts          []artifactProjection `json:"artifacts"`
	}
	projection := description{
		UID: run.GetUid(), Flow: run.GetFlow(), Phase: run.GetPhase(), PlanHash: run.GetPlanHash(), InterpreterVersion: run.GetInterpreterVersion(),
		CreatedAt: formatTimestamp(run.GetCreatedAt()), UpdatedAt: formatTimestamp(run.GetUpdatedAt()), ExecutionPlan: plan.GetExecutionPlanJson(),
		Attempts: make([]attemptProjection, 0, len(attempts.GetAttempts())), Artifacts: make([]artifactProjection, 0, len(artifacts.GetArtifacts())),
	}
	for _, attempt := range attempts.GetAttempts() {
		projection.Attempts = append(projection.Attempts, attemptProjection{
			NodeID: attempt.GetNodeId(), LogicalIteration: attempt.GetLogicalIteration(), Attempt: attempt.GetAttempt(), FrameworkAttempt: attempt.GetFrameworkAttempt(),
			Phase: attempt.GetPhase(), ExitOutcome: attempt.GetExitOutcome(), Error: attempt.GetError(), StartedAt: formatTimestamp(attempt.GetStartedAt()), CompletedAt: formatTimestamp(attempt.GetCompletedAt()),
		})
	}
	for _, artifact := range artifacts.GetArtifacts() {
		projection.Artifacts = append(projection.Artifacts, artifactProjection{
			UID: artifact.GetUid(), NodeID: artifact.GetNodeId(), Attempt: artifact.GetAttempt(), Name: artifact.GetName(), MediaType: artifact.GetMediaType(), SizeBytes: artifact.GetSizeBytes(), SHA256: artifact.GetSha256(),
		})
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return err
	}
	return printDocument(writer, encoded, format)
}

func formatTimestamp(value *timestamppb.Timestamp) string {
	if value == nil {
		return ""
	}
	return value.AsTime().UTC().Format(time.RFC3339Nano)
}

func exportJSONDocuments(data []byte) ([][]byte, error) {
	documents, err := readManifestDocuments(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	result := make([][]byte, 0, len(documents))
	for index, manifest := range documents {
		document, decodeErr := resource.DecodeStrict(manifest.data)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode exported document %d: %w", index+1, decodeErr)
		}
		result = append(result, document.JSON)
	}
	return result, nil
}
