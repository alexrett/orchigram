package tui

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const tuiArtifactPreviewLimit = 2 << 20

func openRunEvents(application *tview.Application, pages *tview.Pages, snapshot liveSnapshot, runUID string, returnFocus tview.Primitive) {
	var content strings.Builder
	for _, event := range snapshot.RunEvents[runUID] {
		payload := strings.TrimSpace(string(event.GetPayloadJson()))
		if len(payload) > 512 {
			payload = payload[:512] + "…"
		}
		_, _ = fmt.Fprintf(&content, "%d  %s  %s  attempt=%d", event.GetSequence(), event.GetNodeId(), event.GetType(), event.GetAttempt())
		if payload != "" && payload != "{}" {
			_, _ = fmt.Fprintf(&content, "  %s", payload)
		}
		content.WriteByte('\n')
	}
	if content.Len() == 0 {
		content.WriteString("No durable events have been observed yet.\n")
	}
	openRunTextPage(application, pages, "run-events", " Events "+short(runUID)+" ", content.String(), returnFocus)
}

func openRunAttempts(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, runUID string, statusView *tview.TextView) {
	operationContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	response, err := client.Runs.ListAttempts(operationContext, &controlv1alpha1.ListAttemptsRequest{RunUid: runUID, Limit: 1000})
	if err != nil {
		statusView.SetText("[red]Unable to load attempt history[-]")
		return
	}
	table := tview.NewTable().SetSelectable(true, false).SetFixed(1, 0)
	for column, value := range []string{"Node", "Iteration", "Attempt", "Framework", "Phase", "Outcome", "Started", "Error"} {
		table.SetCell(0, column, tview.NewTableCell(value).SetTextColor(tcell.ColorYellow).SetSelectable(false))
	}
	for row, attempt := range response.GetAttempts() {
		errorText := attempt.GetError()
		if len(errorText) > 120 {
			errorText = errorText[:120] + "…"
		}
		values := []string{
			attempt.GetNodeId(), fmt.Sprint(attempt.GetLogicalIteration()), fmt.Sprint(attempt.GetAttempt()), fmt.Sprint(attempt.GetFrameworkAttempt()),
			attempt.GetPhase(), attempt.GetExitOutcome(), timestampText(attempt.GetStartedAt()), errorText,
		}
		for column, value := range values {
			table.SetCell(row+1, column, tview.NewTableCell(value))
		}
	}
	table.SetBorder(true).SetTitle(" Attempts " + short(runUID) + " ")
	pages.AddPage("run-attempts", table, true, true)
	application.SetFocus(table)
}

func openRunArtifacts(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, runUID string, logsOnly bool, statusView *tview.TextView) {
	operationContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	response, err := client.Runs.ListArtifacts(operationContext, &controlv1alpha1.ListArtifactsRequest{RunUid: runUID, Limit: 1000})
	if err != nil {
		statusView.SetText("[red]Unable to load artifact metadata[-]")
		return
	}
	artifacts := make([]*controlv1alpha1.ArtifactInfo, 0, len(response.GetArtifacts()))
	for _, artifact := range response.GetArtifacts() {
		if logsOnly && !artifactLooksLikeLog(artifact) {
			continue
		}
		artifacts = append(artifacts, artifact)
	}
	table := tview.NewTable().SetSelectable(true, false).SetFixed(1, 0)
	for column, value := range []string{"Node", "Attempt", "Name", "Media type", "Bytes", "SHA-256"} {
		table.SetCell(0, column, tview.NewTableCell(value).SetTextColor(tcell.ColorYellow).SetSelectable(false))
	}
	for row, artifact := range artifacts {
		values := []string{artifact.GetNodeId(), fmt.Sprint(artifact.GetAttempt()), artifact.GetName(), artifact.GetMediaType(), fmt.Sprint(artifact.GetSizeBytes()), artifact.GetSha256()}
		for column, value := range values {
			table.SetCell(row+1, column, tview.NewTableCell(value))
		}
	}
	if len(artifacts) == 0 {
		table.SetCell(1, 0, tview.NewTableCell("No matching artifacts").SetSelectable(false))
	}
	title := " Artifacts "
	if logsOnly {
		title = " Logs "
	}
	table.SetBorder(true).SetTitle(title + short(runUID) + " ")
	table.SetSelectedFunc(func(row, _ int) {
		if row <= 0 || row > len(artifacts) {
			return
		}
		artifact := artifacts[row-1]
		if !artifactIsText(artifact) {
			modal := tview.NewModal().SetText(fmt.Sprintf("%s\n\n%s, %d bytes\nSHA-256: %s\n\nBinary artifacts are metadata-only in the TUI.", artifact.GetName(), artifact.GetMediaType(), artifact.GetSizeBytes(), artifact.GetSha256())).AddButtons([]string{"Close"})
			modal.SetDoneFunc(func(_ int, _ string) { pages.RemovePage("artifact-metadata"); application.SetFocus(table) })
			pages.AddPage("artifact-metadata", centered(modal, 76, 14), true, true)
			application.SetFocus(modal)
			return
		}
		statusView.SetText("[yellow]Loading artifact preview…[-]")
		go func() {
			content, truncated, loadErr := loadArtifactPreview(ctx, client, artifact.GetUid())
			application.QueueUpdateDraw(func() {
				if loadErr != nil {
					statusView.SetText("[red]Unable to load artifact preview[-]")
					return
				}
				if truncated {
					content += "\n\n[preview truncated at 2 MiB]\n"
				}
				openRunTextPage(application, pages, "artifact-content", " "+artifact.GetName()+" ", content, table)
			})
		}()
	})
	pages.AddPage("run-artifacts", table, true, true)
	application.SetFocus(table)
}

func loadArtifactPreview(ctx context.Context, client *clientpkg.Client, uid string) (string, bool, error) {
	operationContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stream, err := client.Runs.GetArtifact(operationContext, &controlv1alpha1.GetArtifactRequest{Uid: uid})
	if err != nil {
		return "", false, err
	}
	var content bytes.Buffer
	truncated := false
	for {
		chunk, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			return content.String(), truncated, nil
		}
		if receiveErr != nil {
			return "", false, receiveErr
		}
		remaining := tuiArtifactPreviewLimit - content.Len()
		if remaining <= 0 {
			return content.String(), true, nil
		}
		data := chunk.GetData()
		if len(data) > remaining {
			data = data[:remaining]
			truncated = true
		}
		_, _ = content.Write(data)
		if truncated {
			return content.String(), true, nil
		}
	}
}

func openRunTextPage(application *tview.Application, pages *tview.Pages, name, title, content string, returnFocus tview.Primitive) {
	view := tview.NewTextView().SetDynamicColors(false).SetScrollable(true).SetWrap(false).SetText(content)
	view.SetBorder(true).SetTitle(title)
	view.SetDoneFunc(func(_ tcell.Key) {
		pages.RemovePage(name)
		application.SetFocus(returnFocus)
	})
	pages.AddPage(name, view, true, true)
	application.SetFocus(view)
}

func artifactLooksLikeLog(artifact *controlv1alpha1.ArtifactInfo) bool {
	name := strings.ToLower(artifact.GetName())
	return strings.Contains(name, "log") || strings.HasPrefix(artifact.GetMediaType(), "text/")
}

func artifactIsText(artifact *controlv1alpha1.ArtifactInfo) bool {
	return strings.HasPrefix(artifact.GetMediaType(), "text/") || artifact.GetMediaType() == "application/json" || artifact.GetMediaType() == "application/x-ndjson"
}

func timestampText(value *timestamppb.Timestamp) string {
	if value == nil {
		return ""
	}
	return value.AsTime().UTC().Format(time.RFC3339)
}
