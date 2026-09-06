package custody

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func authorityFixture(t *testing.T, command string, a Attempt, native, global int64, omit string) *CommandAuthority {
	t.Helper()
	phase := map[string]string{"native-reserve": "preparing", "native-launch": "launching", "native-result": "running"}[command]
	body := map[string]any{"schema": "ao-capacity-native-result/v1", "ok": true, "command": command, "admitted": true, "write_authorized": false, "registry_generation": global, "issue_body": "accepted current ticket body", "claim": map[string]any{"claim_id": a.ClaimID, "executor": "ao", "project": a.Project, "owner": a.SessionID, "native": map[string]any{"attempt_id": a.AttemptID, "session_id": a.SessionID, "operation": a.Operation, "phase": phase, "generation": native, "scope": map[string]any{"paths": []string{"src/**"}}, "source": map[string]any{"evidence": map[string]any{"route": map[string]any{"harness": "codex", "model": "gpt-6-astra", "reasoningEffort": "high", "fallback": "none"}}}}}}
	delete(body, omit)
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "authority")
	script := "#!/bin/sh\nprintf '%s' \"$3\" > \"$AO_DATA_DIR/request.json\"\ncat <<'AO_RESULT'\n" + string(b) + "\nAO_RESULT\n"
	if err = os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return &CommandAuthority{dataDir: dir, executable: path}
}
func TestAuthorityDistinguishesGlobalAndClaimGenerations(t *testing.T) {
	a := Attempt{Project: "p", SessionID: "p-1", AttemptID: "attempt", Operation: "spawn", ClaimID: "claim", ClaimGeneration: 2}
	authority := authorityFixture(t, "native-launch", a, 3, 17, "")
	out, err := authority.Transition(context.Background(), "native-launch", a, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.Generation != 17 || out.Claim.Native.Generation != 3 {
		t.Fatal("distinct counters lost")
	}
	b, err := os.ReadFile(filepath.Join(authority.dataDir, "request.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"expected_generation":2`) {
		t.Fatalf("expected wrong authority generation: %s", b)
	}
}
func TestAuthorityRejectsMissingAuthorizationAndLegacyIdentity(t *testing.T) {
	a := Attempt{Project: "p", SessionID: "p-1", AttemptID: "attempt", Operation: "spawn", ClaimID: "claim"}
	for _, key := range []string{"write_authorized", "claim", "issue_body", "registry_generation"} {
		t.Run(key, func(t *testing.T) {
			authority := authorityFixture(t, "native-reserve", a, 1, 4, key)
			if _, err := authority.Transition(context.Background(), "native-reserve", a, nil); err == nil {
				t.Fatal("missing authority evidence accepted")
			}
		})
	}
}
func TestCanonicalManagedIssueBinding(t *testing.T) {
	if n, err := IssueNumber("github:nic-kahuna/reflex#715", "nic-kahuna/reflex"); err != nil || n != 715 {
		t.Fatalf("canonical id: %d %v", n, err)
	}
	for _, id := range []string{"715", "github:other/reflex#715", "github:nic-kahuna/reflex#0", "github:nic-kahuna/reflex#715#2"} {
		if _, err := IssueNumber(id, "nic-kahuna/reflex"); err == nil {
			t.Fatalf("invalid id accepted %s", id)
		}
	}
}
