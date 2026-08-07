package pluginruntime

import "testing"

func TestFinalAgentTextNormalizesClaudeAndCodexJSONL(t *testing.T) {
	t.Parallel()
	claude := []byte("{\"type\":\"assistant\"}\n{\"type\":\"result\",\"result\":\"daily note\"}\n")
	if text := finalAgentText(claude); text != "daily note" {
		t.Fatalf("Claude text=%q", text)
	}
	codex := []byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"implementation plan\"}}\n")
	if text := finalAgentText(codex); text != "implementation plan" {
		t.Fatalf("Codex text=%q", text)
	}
}
