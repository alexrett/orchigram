package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const pluginUploadChunkSize = 1 << 20

var resourceCreateTemplates = map[string]string{
	"Flow": `apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata:
  name: new-flow
spec:
  nodes:
    - id: start
      uses: core.noop
    - id: finish
      uses: core.noop
  edges:
    - from: start
      to: finish
`,
	"Trigger": `apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata:
  name: new-trigger
spec:
  flow: flow-name
  enabled: false
  schedule:
    cron: "0 9 * * 1-5"
    timezone: UTC
`,
	"Repository": `apiVersion: orchigram.dev/v1alpha1
kind: Repository
metadata:
  name: new-repository
spec:
  cloneURL: https://github.com/owner/repository.git
  defaultBranch: main
  workspacePolicy: isolated
`,
	"AgentProfile": `apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata:
  name: new-agent
spec:
  type: command
  executable: /usr/bin/true
  sandbox: read-only
`,
	"SecretRef": `apiVersion: orchigram.dev/v1alpha1
kind: SecretRef
metadata:
  name: new-secret-ref
spec:
  backend: env
  key: ORCHIGRAM_SECRET_NAME
`,
}

func openCreateResource(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, notifications *tview.TextView, returnFocus tview.Primitive) {
	kinds := make([]string, 0, len(resourceCreateTemplates))
	for kind := range resourceCreateTemplates {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	list := tview.NewList().ShowSecondaryText(false).SetUseStyleTags(false, false)
	list.SetBorder(true).SetTitle(" Create resource ")
	for _, kind := range kinds {
		kind := kind
		list.AddItem(kind, "", 0, func() {
			pages.RemovePage("resource-create-kind")
			openResourceCreateEditor(ctx, application, pages, client, kind, notifications, returnFocus)
		})
	}
	pages.AddPage("resource-create-kind", centered(list, 42, len(kinds)+4), true, true)
	application.SetFocus(list)
}

func openResourceCreateEditor(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, kind string, notifications *tview.TextView, returnFocus tview.Primitive) {
	editor := tview.NewTextArea().SetText(resourceCreateTemplates[kind], false).SetWrap(false)
	editor.SetBorder(true).SetTitle(" New " + kind + " (strict YAML) ")
	footer := tview.NewTextView().SetDynamicColors(true).SetText(" [yellow]Ctrl-S[-] validate & apply   [yellow]Esc[-] cancel")
	apply := func() {
		operationContext, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		response, err := client.Resources.Validate(operationContext, &controlv1alpha1.ApplyRequest{Document: []byte(editor.GetText())})
		if err != nil {
			notifications.SetText("[red]Validation failed: " + escape(err.Error()))
			return
		}
		if diagnostic := firstErrorDiagnostic(response.GetDiagnostics()); diagnostic != nil {
			notifications.SetText("[red]" + escape(diagnostic.GetPath()+": "+diagnostic.GetMessage()))
			return
		}
		response, err = client.Resources.Apply(operationContext, &controlv1alpha1.ApplyRequest{Document: []byte(editor.GetText())})
		if err != nil {
			notifications.SetText("[red]Create conflict or validation failure: " + escape(err.Error()))
			return
		}
		if diagnostic := firstErrorDiagnostic(response.GetDiagnostics()); diagnostic != nil {
			notifications.SetText("[red]" + escape(diagnostic.GetPath()+": "+diagnostic.GetMessage()))
			return
		}
		notifications.SetText(fmt.Sprintf("[green]Created %s/%s[-]", escape(response.GetResource().GetKey().GetKind()), escape(response.GetResource().GetKey().GetName())))
		pages.RemovePage("resource-create-editor")
		application.SetFocus(returnFocus)
	}
	editor.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyCtrlS {
			apply()
			return nil
		}
		return event
	})
	page := tview.NewFlex().SetDirection(tview.FlexRow).AddItem(editor, 0, 1, true).AddItem(footer, 1, 0, false)
	pages.AddPage("resource-create-editor", page, true, true)
	application.SetFocus(editor)
}

func openDeleteResource(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, document *controlv1alpha1.ResourceDocument, notifications *tview.TextView, returnFocus tview.Primitive) {
	key := document.GetKey()
	modal := tview.NewModal().SetText(tview.Escape(fmt.Sprintf("Delete %s/%s?\n\nThis uses resourceVersion %d and will fail rather than overwrite a concurrent change.", key.GetKind(), resourceDisplayName(key), document.GetResourceVersion()))).AddButtons([]string{"Delete", "Cancel"})
	modal.SetDoneFunc(func(buttonIndex int, _ string) {
		if buttonIndex != 0 {
			pages.RemovePage("resource-delete")
			application.SetFocus(returnFocus)
			return
		}
		operationContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_, err := client.Resources.Delete(operationContext, &controlv1alpha1.DeleteRequest{Key: cloneMessage(key), ExpectedResourceVersion: document.GetResourceVersion()})
		if err != nil {
			notifications.SetText("[red]CAS conflict or delete failure: " + escape(err.Error()))
			return
		}
		notifications.SetText("[green]Deleted " + escape(key.GetKind()+"/"+resourceDisplayName(key)))
		pages.RemovePage("resource-delete")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("resource-delete", centered(modal, 70, 13), true, true)
	application.SetFocus(modal)
}

func openStartFlow(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, document *controlv1alpha1.ResourceDocument, notifications *tview.TextView, returnFocus tview.Primitive) {
	form := tview.NewForm().AddInputField("Input JSON", "{}", 64, nil, nil).AddInputField("Idempotency key", "", 48, nil, nil)
	form.SetBorder(true).SetTitle(" Start Flow/" + document.GetKey().GetName() + " ")
	form.AddButton("Start", func() {
		input := []byte(formText(form, "Input JSON"))
		if !json.Valid(input) {
			notifications.SetText("[red]Input must be valid JSON")
			return
		}
		operationContext, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		run, err := client.Runs.Start(operationContext, &controlv1alpha1.StartRunRequest{Flow: document.GetKey().GetName(), InputJson: input, IdempotencyKey: formText(form, "Idempotency key")})
		if err != nil {
			notifications.SetText("[red]Unable to start Flow: " + escape(err.Error()))
			return
		}
		notifications.SetText("[green]Accepted run " + escape(run.GetUid()))
		pages.RemovePage("flow-start")
		application.SetFocus(returnFocus)
	}).AddButton("Cancel", func() {
		pages.RemovePage("flow-start")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("flow-start", centered(form, 76, 12), true, true)
	application.SetFocus(form)
}

func openPluginInstall(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, notifications *tview.TextView, returnFocus tview.Primitive) {
	form := tview.NewForm().AddInputField("Bundle path", "", 64, nil, nil)
	form.SetBorder(true).SetTitle(" Install plugin bundle ")
	form.AddButton("Upload & install", func() {
		path := filepath.Clean(formText(form, "Bundle path"))
		bundle, err := os.ReadFile(path) //nolint:gosec // The local operator explicitly selects the bundle path.
		if err != nil {
			notifications.SetText("[red]Unable to read plugin bundle")
			return
		}
		if len(bundle) == 0 {
			notifications.SetText("[red]Plugin bundle is empty")
			return
		}
		operationContext, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		installed, err := uploadPluginBundle(operationContext, client, bundle)
		if err != nil {
			notifications.SetText("[red]Plugin install failed: " + escape(err.Error()))
			return
		}
		notifications.SetText(fmt.Sprintf("[green]Installed %s:%s[-]", escape(installed.GetName()), escape(installed.GetVersion())))
		pages.RemovePage("plugin-install")
		application.SetFocus(returnFocus)
	}).AddButton("Cancel", func() {
		pages.RemovePage("plugin-install")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("plugin-install", centered(form, 76, 10), true, true)
	application.SetFocus(form)
}

func uploadPluginBundle(ctx context.Context, client *clientpkg.Client, bundle []byte) (*controlv1alpha1.PluginInstallResponse, error) {
	stream, err := client.Plugins.Install(ctx)
	if err != nil {
		return nil, err
	}
	for offset := 0; offset < len(bundle); offset += pluginUploadChunkSize {
		end := min(offset+pluginUploadChunkSize, len(bundle))
		if err := stream.Send(&controlv1alpha1.PluginUploadRequest{BundleChunk: bundle[offset:end], Final: end == len(bundle)}); err != nil {
			return nil, err
		}
	}
	return stream.CloseAndRecv()
}

func openPluginRollback(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, plugin *controlv1alpha1.PluginInfo, plugins []*controlv1alpha1.PluginInfo, notifications *tview.TextView, returnFocus tview.Primitive) {
	candidates := make([]*controlv1alpha1.PluginInfo, 0)
	for _, candidate := range plugins {
		if candidate.GetName() == plugin.GetName() && candidate.GetVersion() != plugin.GetVersion() {
			candidates = append(candidates, candidate)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].GetVersion() > candidates[j].GetVersion() })
	if len(candidates) == 0 {
		notifications.SetText("[yellow]No previous installed version is available")
		return
	}
	list := tview.NewList().ShowSecondaryText(false).SetUseStyleTags(false, false)
	list.SetBorder(true).SetTitle(" Activate previous " + tview.Escape(plugin.GetName()) + " version ")
	for _, candidate := range candidates {
		candidate := candidate
		list.AddItem(candidate.GetVersion()+"  ["+candidate.GetState()+"]", "", 0, func() {
			operationContext, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if _, err := client.Plugins.Enable(operationContext, &controlv1alpha1.PluginRequest{Name: candidate.GetName(), Version: candidate.GetVersion()}); err != nil {
				notifications.SetText("[red]Rollback failed: " + escape(err.Error()))
				return
			}
			notifications.SetText("[green]Activated " + escape(candidate.GetName()+":"+candidate.GetVersion()))
			pages.RemovePage("plugin-rollback")
			application.SetFocus(returnFocus)
		})
	}
	pages.AddPage("plugin-rollback", centered(list, 62, min(18, len(candidates)+4)), true, true)
	application.SetFocus(list)
}
