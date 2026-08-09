package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/rivo/tview"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type actionConfigField struct {
	name     string
	typeName string
	required bool
}

func openFlowNodeForm(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, document *controlv1alpha1.ResourceDocument, planNode flow.PlanNode, notifications *tview.TextView, returnFocus tview.Primitive) {
	definition, err := resource.DecodeFlow(document.GetJson())
	if err != nil {
		notifications.SetText("[red]Unable to decode the selected Flow")
		return
	}
	index := -1
	for candidate := range definition.Spec.Nodes {
		if definition.Spec.Nodes[candidate].ID == planNode.ID {
			index = candidate
			break
		}
	}
	if index < 0 {
		notifications.SetText("[red]The selected node no longer exists")
		return
	}
	node := definition.Spec.Nodes[index]
	form := tview.NewForm()
	form.SetBorder(true).SetTitle(" Edit node " + tview.Escape(node.ID) + " ")
	form.AddInputField("ID", node.ID, 48, nil, nil)
	form.AddInputField("Name", node.Name, 48, nil, nil)
	form.AddInputField("Uses", node.Uses, 48, nil, nil)
	form.AddInputField("Timeout", node.Timeout, 24, nil, nil)
	retryLimit, retryBackoff := "", ""
	if node.Retry != nil {
		retryLimit, retryBackoff = strconv.Itoa(node.Retry.Limit), node.Retry.Backoff
	}
	form.AddInputField("Retry limit", retryLimit, 12, nil, nil)
	form.AddInputField("Retry backoff", retryBackoff, 24, nil, nil)
	loopMax := ""
	if node.Loop != nil {
		loopMax = strconv.Itoa(node.Loop.MaxIterations)
	}
	form.AddInputField("Loop max", loopMax, 12, nil, nil)
	configFields, schemaComplete := actionConfigFields(planNode)
	if schemaComplete {
		for _, descriptor := range configFields {
			form.AddInputField(configFieldLabel(descriptor), scalarText(node.With[descriptor.name]), 48, nil, nil)
		}
	} else {
		encoded, _ := json.Marshal(node.With)
		if len(encoded) == 0 || string(encoded) == "null" {
			encoded = []byte(`{}`)
		}
		form.AddInputField("Config JSON", string(encoded), 64, nil, nil)
	}
	form.AddButton("Validate & apply", func() {
		updated := node
		updated.ID = formText(form, "ID")
		updated.Name = formText(form, "Name")
		updated.Uses = formText(form, "Uses")
		updated.Timeout = formText(form, "Timeout")
		limitText, backoff := formText(form, "Retry limit"), formText(form, "Retry backoff")
		if limitText == "" && backoff == "" {
			updated.Retry = nil
		} else {
			limit, parseErr := optionalPositiveInt(limitText)
			if parseErr != nil {
				notifications.SetText("[red]Retry limit must be a non-negative integer")
				return
			}
			updated.Retry = &resource.RetryPolicy{Limit: limit, Backoff: backoff}
		}
		loopText := formText(form, "Loop max")
		if loopText == "" {
			updated.Loop = nil
		} else {
			loop, parseErr := optionalPositiveInt(loopText)
			if parseErr != nil || loop == 0 {
				notifications.SetText("[red]Loop max must be a positive integer")
				return
			}
			updated.Loop = &resource.LoopPolicy{MaxIterations: loop}
		}
		if schemaComplete {
			config := cloneConfig(node.With)
			for _, descriptor := range configFields {
				value, present, parseErr := parseConfigField(descriptor, formText(form, configFieldLabel(descriptor)))
				if parseErr != nil {
					notifications.SetText("[red]" + escape(parseErr.Error()))
					return
				}
				if present {
					config[descriptor.name] = value
				} else {
					delete(config, descriptor.name)
				}
			}
			updated.With = config
		} else {
			var config map[string]any
			if decodeErr := json.Unmarshal([]byte(formText(form, "Config JSON")), &config); decodeErr != nil {
				notifications.SetText("[red]Config JSON must be a JSON object")
				return
			}
			updated.With = config
		}
		oldID := definition.Spec.Nodes[index].ID
		definition.Spec.Nodes[index] = updated
		if oldID != updated.ID {
			for edgeIndex := range definition.Spec.Edges {
				if definition.Spec.Edges[edgeIndex].From == oldID {
					definition.Spec.Edges[edgeIndex].From = updated.ID
				}
				if definition.Spec.Edges[edgeIndex].To == oldID {
					definition.Spec.Edges[edgeIndex].To = updated.ID
				}
			}
		}
		if !applyFlowDefinition(ctx, client, document, definition, notifications) {
			return
		}
		updateFlowGraph(ctx, client, returnFocus, document, notifications)
		pages.RemovePage("flow-node-form")
		application.SetFocus(returnFocus)
	}).AddButton("Cancel", func() {
		pages.RemovePage("flow-node-form")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("flow-node-form", centered(form, 76, min(22, form.GetFormItemCount()+8)), true, true)
	application.SetFocus(form)
}

func openFlowEdgeForm(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, document *controlv1alpha1.ResourceDocument, planEdge flow.PlanEdge, planIndex int, notifications *tview.TextView, returnFocus tview.Primitive) {
	definition, err := resource.DecodeFlow(document.GetJson())
	if err != nil {
		notifications.SetText("[red]Unable to decode the selected Flow")
		return
	}
	index := sourceEdgeIndex(definition.Spec.Edges, planEdge, planIndex)
	if index < 0 {
		notifications.SetText("[red]The selected edge no longer exists")
		return
	}
	edge := definition.Spec.Edges[index]
	form := tview.NewForm().AddInputField("From", edge.From, 48, nil, nil).AddInputField("To", edge.To, 48, nil, nil).AddInputField("When (CEL)", edge.When, 64, nil, nil)
	form.SetBorder(true).SetTitle(" Edit edge " + tview.Escape(edge.From) + " -> " + tview.Escape(edge.To) + " ")
	form.AddButton("Validate & apply", func() {
		definition.Spec.Edges[index] = resource.FlowEdge{From: formText(form, "From"), To: formText(form, "To"), When: formText(form, "When (CEL)")}
		if !applyFlowDefinition(ctx, client, document, definition, notifications) {
			return
		}
		updateFlowGraph(ctx, client, returnFocus, document, notifications)
		pages.RemovePage("flow-edge-form")
		application.SetFocus(returnFocus)
	}).AddButton("Cancel", func() {
		pages.RemovePage("flow-edge-form")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("flow-edge-form", centered(form, 76, 13), true, true)
	application.SetFocus(form)
}

func updateFlowGraph(ctx context.Context, client *clientpkg.Client, returnFocus tview.Primitive, document *controlv1alpha1.ResourceDocument, notifications *tview.TextView) {
	graph, ok := returnFocus.(*Graph)
	if !ok {
		return
	}
	plan, err := compileFlowPlan(ctx, client, document)
	if err != nil {
		notifications.SetText("[red]Applied the Flow but could not refresh its graph: " + escape(err.Error()))
		return
	}
	graph.SetPlan(plan)
}

func compileFlowPlan(ctx context.Context, client *clientpkg.Client, document *controlv1alpha1.ResourceDocument) (flow.ExecutionPlan, error) {
	operationContext, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	response, err := client.Flows.Compile(operationContext, &controlv1alpha1.CompileRequest{Flow: document.GetJson()})
	if err != nil {
		return flow.ExecutionPlan{}, err
	}
	if diagnostic := firstErrorDiagnostic(response.GetDiagnostics()); diagnostic != nil {
		return flow.ExecutionPlan{}, fmt.Errorf("%s: %s", diagnostic.GetPath(), diagnostic.GetMessage())
	}
	if len(response.GetExecutionPlanJson()) == 0 {
		return flow.ExecutionPlan{}, fmt.Errorf("the daemon returned no execution plan")
	}
	var plan flow.ExecutionPlan
	if err := json.Unmarshal(response.GetExecutionPlanJson(), &plan); err != nil {
		return flow.ExecutionPlan{}, fmt.Errorf("decode execution plan: %w", err)
	}
	return plan, nil
}

func applyFlowDefinition(ctx context.Context, client *clientpkg.Client, document *controlv1alpha1.ResourceDocument, definition resource.Flow, notifications *tview.TextView) bool {
	definition.Status = nil
	encoded, err := json.Marshal(definition)
	if err != nil {
		notifications.SetText("[red]Unable to encode the Flow edit")
		return false
	}
	operationContext, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	request := &controlv1alpha1.ApplyRequest{Document: encoded, ExpectedResourceVersion: document.GetResourceVersion()}
	validated, err := client.Resources.Validate(operationContext, request)
	if err != nil {
		notifications.SetText("[red]Flow validation failed: " + escape(err.Error()))
		return false
	}
	if diagnostic := firstErrorDiagnostic(validated.GetDiagnostics()); diagnostic != nil {
		notifications.SetText("[red]" + escape(diagnostic.GetPath()+": "+diagnostic.GetMessage()))
		return false
	}
	var applied *controlv1alpha1.ApplyResponse
	for attempt := 0; attempt < 3; attempt++ {
		applied, err = client.Resources.Apply(operationContext, request)
		if err == nil || status.Code(err) != codes.Aborted {
			break
		}
		current, getErr := client.Resources.Get(operationContext, &controlv1alpha1.GetRequest{Key: cloneMessage(document.GetKey())})
		if getErr != nil || !sameFlowGeneration(document, current) {
			break
		}
		currentDefinition, decodeErr := resource.DecodeFlow(current.GetJson())
		if decodeErr != nil {
			break
		}
		definition.Metadata = currentDefinition.Metadata
		encoded, err = json.Marshal(definition)
		if err != nil {
			break
		}
		request = &controlv1alpha1.ApplyRequest{Document: encoded, ExpectedResourceVersion: current.GetResourceVersion()}
	}
	if err != nil {
		notifications.SetText("[red]CAS conflict or validation failure: " + escape(err.Error()))
		return false
	}
	if diagnostic := firstErrorDiagnostic(applied.GetDiagnostics()); diagnostic != nil {
		notifications.SetText("[red]" + escape(diagnostic.GetPath()+": "+diagnostic.GetMessage()))
		return false
	}
	*document = *cloneMessage(applied.GetResource())
	notifications.SetText(fmt.Sprintf("[green]Applied Flow generation %d[-]", document.GetGeneration()))
	return true
}

func sameFlowGeneration(selected, current *controlv1alpha1.ResourceDocument) bool {
	return selected != nil && current != nil && selected.GetKey() != nil && current.GetKey() != nil &&
		selected.GetKey().GetUid() == current.GetKey().GetUid() && selected.GetGeneration() == current.GetGeneration()
}

func actionConfigFields(node flow.PlanNode) ([]actionConfigField, bool) {
	if node.Contract == nil || len(node.Contract.ConfigSchema) == 0 {
		return nil, false
	}
	var schema struct {
		Properties map[string]struct {
			Type any `json:"type"`
		} `json:"properties"`
		Required []string `json:"required"`
	}
	if err := json.Unmarshal(node.Contract.ConfigSchema, &schema); err != nil {
		return nil, false
	}
	required := make(map[string]bool, len(schema.Required))
	for _, name := range schema.Required {
		required[name] = true
	}
	result := make([]actionConfigField, 0, len(schema.Properties))
	for name, property := range schema.Properties {
		typeName, ok := property.Type.(string)
		if !ok || (typeName != "string" && typeName != "boolean" && typeName != "integer" && typeName != "number") {
			return nil, false
		}
		result = append(result, actionConfigField{name: name, typeName: typeName, required: required[name]})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].name < result[j].name })
	return result, true
}

func parseConfigField(descriptor actionConfigField, text string) (any, bool, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		if descriptor.required {
			return nil, false, fmt.Errorf("config / %s is required", descriptor.name)
		}
		return nil, false, nil
	}
	switch descriptor.typeName {
	case "boolean":
		value, err := strconv.ParseBool(text)
		return value, true, err
	case "integer":
		value, err := strconv.ParseInt(text, 10, 64)
		return value, true, err
	case "number":
		value, err := strconv.ParseFloat(text, 64)
		return value, true, err
	default:
		return text, true, nil
	}
}

func configFieldLabel(descriptor actionConfigField) string {
	label := "Config / " + descriptor.name
	if descriptor.required {
		label += " *"
	}
	return label
}

func sourceEdgeIndex(edges []resource.FlowEdge, selected flow.PlanEdge, planIndex int) int {
	if planIndex >= 0 && planIndex < len(edges) {
		edge := edges[planIndex]
		if edge.From == selected.From && edge.To == selected.To && edge.When == selected.Condition {
			return planIndex
		}
	}
	for index, edge := range edges {
		if edge.From == selected.From && edge.To == selected.To && edge.When == selected.Condition {
			return index
		}
	}
	return -1
}

func firstErrorDiagnostic(diagnostics []*controlv1alpha1.Diagnostic) *controlv1alpha1.Diagnostic {
	for _, diagnostic := range diagnostics {
		if diagnostic.GetSeverity() == controlv1alpha1.Diagnostic_SEVERITY_ERROR || diagnostic.GetSeverity() == controlv1alpha1.Diagnostic_SEVERITY_UNSPECIFIED {
			return diagnostic
		}
	}
	return nil
}

func formText(form *tview.Form, label string) string {
	return strings.TrimSpace(form.GetFormItemByLabel(label).(*tview.InputField).GetText())
}

func optionalPositiveInt(text string) (int, error) {
	if strings.TrimSpace(text) == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(text)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("invalid non-negative integer")
	}
	return value, nil
}

func cloneConfig(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
