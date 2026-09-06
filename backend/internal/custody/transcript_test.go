package custody

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func checkpointFixture(t *testing.T, lines ...string) Attempt {
	t.Helper()
	workspace := t.TempDir()
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	meta, _ := json.Marshal(map[string]any{"type": "session_meta", "payload": map[string]any{"id": "provider", "cwd": workspace}})
	raw := string(meta) + "\n" + strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	return Attempt{ProviderID: "provider", Workspace: workspace, Transcript: path, TurnID: "drain", StopHookTurn: "drain"}
}
func checkpointEvent(kind, turn string) string {
	b, _ := json.Marshal(map[string]any{"type": "event_msg", "payload": map[string]any{"type": kind, "turn_id": turn}})
	return string(b)
}
func checkpointTool(id, name, input string) string {
	b, _ := json.Marshal(map[string]any{"type": "response_item", "payload": map[string]any{"type": "custom_tool_call", "call_id": id, "name": name, "input": input}})
	return string(b)
}
func checkpointOutput(id string, output any) string {
	b, _ := json.Marshal(map[string]any{"type": "response_item", "payload": map[string]any{"type": "custom_tool_call_output", "call_id": id, "output": output}})
	return string(b)
}
func checkpointBlocks(header, body string) any {
	return []map[string]string{{"type": "input_text", "text": header}, {"type": "input_text", "text": body}}
}
func checkpointCommand(pid, turn, status string, code any) string {
	b, _ := json.Marshal(map[string]any{"type": "event_msg", "payload": map[string]any{"type": "item_completed", "thread_id": "provider", "turn_id": turn, "item": map[string]any{"type": "CommandExecution", "id": "exec-" + pid, "process_id": pid, "status": status, "exit_code": code}}})
	return string(b)
}
func checkpointDrainLines() []string {
	// Faithful reduced structure from the actual 0.153.3 cooperative fixture:
	// an interrupted poll returned, but only original CommandExecution143
	// later settled process46888 before the coordinator's completed turn.
	return []string{
		checkpointEvent("task_started", "original"),
		checkpointTool("launch", "exec", `text(await tools.exec_command({cmd:"worker",yield_time_ms:30000}));`),
		checkpointOutput("launch", checkpointBlocks("Script completed\nWall time 30.9 seconds\nOutput:\n", `{"session_id":46888,"output":"worker running"}`)),
		checkpointTool("poll", "exec", `text(await tools.write_stdin({session_id:46888,chars:"",yield_time_ms:300000}));`),
		checkpointOutput("poll", "aborted by user after 0.1s"),
		checkpointEvent("turn_aborted", "original"),
		checkpointEvent("task_started", "drain"),
		checkpointCommand("46888", "original", "failed", 143),
		checkpointTool("lookup", "exec", `text(await tools.write_stdin({session_id:46888,chars:"",yield_time_ms:1000}));`),
		checkpointOutput("lookup", checkpointBlocks("Script failed\nWall time 0.0 seconds\nOutput:\n", "Script error:\nwrite_stdin failed: Unknown process id 46888")),
		checkpointEvent("task_complete", "drain"),
	}
}
func TestCheckpointCooperativeDrainRequiresOriginalCompletion(t *testing.T) {
	lines := checkpointDrainLines()
	a := checkpointFixture(t, lines...)
	cp, err := ReadCheckpoint(a)
	if err != nil || !cp.Complete || cp.Interrupted || len(cp.Outstanding) != 0 {
		t.Fatalf("checkpoint=%+v error=%v", cp, err)
	}
	raw, _ := os.ReadFile(a.Transcript)
	sum := sha256.Sum256(raw)
	if cp.ContextSHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("hash does not bind exact transcript bytes")
	}
	without := append([]string{}, lines[:7]...)
	without = append(without, lines[8:]...)
	a = checkpointFixture(t, without...)
	cp, err = ReadCheckpoint(a)
	if err != nil || cp.Complete || !reflect.DeepEqual(cp.Outstanding, []string{"process:46888", "tool:lookup", "tool:poll"}) {
		t.Fatalf("unsettled checkpoint=%+v error=%v", cp, err)
	}
}
func TestCheckpointAbortedTurnAndStaleStopNeverComplete(t *testing.T) {
	a := checkpointFixture(t, checkpointEvent("task_started", "drain"), checkpointEvent("turn_aborted", "drain"))
	cp, err := ReadCheckpoint(a)
	if err != nil || cp.Complete || !cp.Interrupted || cp.TurnID != "drain" {
		t.Fatalf("checkpoint=%+v error=%v", cp, err)
	}
	a = checkpointFixture(t, checkpointEvent("task_started", "drain"), checkpointEvent("task_complete", "drain"))
	a.StopHookTurn = "original"
	cp, err = ReadCheckpoint(a)
	if err != nil || cp.Complete {
		t.Fatalf("stale callback accepted: %+v %v", cp, err)
	}
	a.StopHookTurn = "drain"
	a.TurnID = "original"
	cp, err = ReadCheckpoint(a)
	if err != nil || cp.Complete {
		t.Fatalf("stale turn accepted: %+v %v", cp, err)
	}
}
func TestCheckpointNewTurnSupersedesCompletedBoundary(t *testing.T) {
	a := checkpointFixture(t, checkpointEvent("task_started", "drain"), checkpointEvent("task_complete", "drain"), checkpointEvent("task_started", "later"))
	cp, err := ReadCheckpoint(a)
	if err != nil || cp.Complete || cp.Interrupted || cp.TurnID != "later" {
		t.Fatalf("checkpoint=%+v error=%v", cp, err)
	}
}
func TestCheckpointCellsAndUnknownChildrenRemainOutstanding(t *testing.T) {
	for _, finish := range []bool{false, true} {
		lines := []string{checkpointEvent("task_started", "drain"), checkpointTool("cell", "exec", "await slowOperation()"), checkpointOutput("cell", checkpointBlocks("Script running with cell ID cell_1\nWall time 30.0 seconds\nOutput:\n", ""))}
		if finish {
			lines = append(lines, checkpointTool("wait", "wait", `{"cell_id":"cell_1"}`), checkpointOutput("wait", checkpointBlocks("Script completed\nWall time 1.0 seconds\nOutput:\n", "")))
		}
		lines = append(lines, checkpointEvent("task_complete", "drain"))
		cp, err := ReadCheckpoint(checkpointFixture(t, lines...))
		if err != nil || cp.Complete != finish {
			t.Fatalf("finish=%v checkpoint=%+v error=%v", finish, cp, err)
		}
	}
	a := checkpointFixture(t, checkpointEvent("task_started", "drain"), checkpointTool("child", "exec", "launchChild()"), checkpointOutput("child", checkpointBlocks("Script completed\nWall time 1.0 seconds\nOutput:\n", `{"threadId":"child"}`)), checkpointEvent("task_complete", "drain"))
	cp, err := ReadCheckpoint(a)
	if err != nil || cp.Complete || !reflect.DeepEqual(cp.Outstanding, []string{"child_output:child"}) {
		t.Fatalf("checkpoint=%+v error=%v", cp, err)
	}
}
func TestCheckpointPendingToolAndInvalidTerminalStatusRemainOutstanding(t *testing.T) {
	for _, code := range []any{nil, true, "143", 1.5} {
		lines := checkpointDrainLines()
		lines[7] = checkpointCommand("46888", "original", "failed", code)
		cp, err := ReadCheckpoint(checkpointFixture(t, lines...))
		if err != nil || cp.Complete {
			t.Fatalf("code=%v checkpoint=%+v error=%v", code, cp, err)
		}
	}
	a := checkpointFixture(t, checkpointEvent("task_started", "drain"), checkpointTool("pending", "exec", "await running()"), checkpointEvent("task_complete", "drain"))
	cp, err := ReadCheckpoint(a)
	if err != nil || cp.Complete || !reflect.DeepEqual(cp.Outstanding, []string{"tool:pending"}) {
		t.Fatalf("checkpoint=%+v error=%v", cp, err)
	}
}
func TestCheckpointDoesNotTreatStdoutOrWrapperExitAsProcessCompletion(t *testing.T) {
	lines := checkpointDrainLines()
	lines = append(lines[:7], lines[8:]...)
	lines = append(lines, checkpointTool("fake", "exec", "printReceipt()"), checkpointOutput("fake", checkpointBlocks("Script completed\nWall time 0.1 seconds\nOutput:\n", `{"exit_code":0,"output":"{\"process_id\":\"46888\",\"exit_code\":143}"}`)))
	cp, err := ReadCheckpoint(checkpointFixture(t, lines...))
	if err != nil || cp.Complete || !strings.Contains(strings.Join(cp.Outstanding, ","), "process:46888") {
		t.Fatalf("checkpoint=%+v error=%v", cp, err)
	}
}
func TestCheckpointInvalidIdentityAndPartialFileFail(t *testing.T) {
	for _, kind := range []string{"provider", "workspace", "symlink", "partial", "malformed", "unknown", "duplicate-output"} {
		t.Run(kind, func(t *testing.T) {
			a := checkpointFixture(t, checkpointEvent("task_started", "drain"), checkpointEvent("task_complete", "drain"))
			switch kind {
			case "provider":
				a.ProviderID = "other"
			case "workspace":
				a.Workspace = t.TempDir()
			case "symlink":
				p := a.Transcript + ".link"
				if err := os.Symlink(a.Transcript, p); err != nil {
					t.Fatal(err)
				}
				a.Transcript = p
			case "partial":
				raw, _ := os.ReadFile(a.Transcript)
				if err := os.WriteFile(a.Transcript, raw[:len(raw)-1], 0600); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				if err := os.WriteFile(a.Transcript, []byte("broken\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "unknown":
				a = checkpointFixture(t, `{"type":"future_execution","payload":{}}`)
			case "duplicate-output":
				a = checkpointFixture(t, checkpointTool("one", "exec", ""), checkpointOutput("one", "aborted"), checkpointOutput("one", "aborted"))
			}
			cp, err := ReadCheckpoint(a)
			if err == nil || cp.Complete {
				t.Fatalf("accepted invalid transcript %+v %v", cp, err)
			}
		})
	}
}

func TestCheckpointRejectsAmbiguousControlAndReusedProcessEvidence(t *testing.T) {
	lines := checkpointDrainLines()
	// A second command reusing a numeric tool handle must not inherit the
	// completion of the first command. The known completed poll is different.
	lines = append(lines[:len(lines)-1], checkpointTool("reuse", "exec", `text(await tools.exec_command({cmd:"new worker"}));`), checkpointOutput("reuse", checkpointBlocks("Script completed\nWall time 0.1 seconds\nOutput:\n", `{"session_id":46888}`)), checkpointEvent("task_complete", "drain"))
	cp, err := ReadCheckpoint(checkpointFixture(t, lines...))
	if err != nil || cp.Complete || !strings.Contains(strings.Join(cp.Outstanding, ","), "process:46888") {
		t.Fatalf("reused process=%+v error=%v", cp, err)
	}
	a := checkpointFixture(t, checkpointEvent("task_started", "drain"), `{"type":"event_msg","payload":{"type":"turn_aborted","type":"task_complete","turn_id":"drain"}}`)
	if cp, err := ReadCheckpoint(a); err == nil || cp.Complete {
		t.Fatalf("duplicate control accepted %+v %v", cp, err)
	}
	a = checkpointFixture(t, checkpointEvent("task_started", "drain"), `{"type":"event_msg","payload":{"type":"item_completed","thread_id":"provider","turn_id":"drain","item":{"type":"FutureBackgroundJob","id":"future"}}}`, checkpointEvent("task_complete", "drain"))
	cp, err = ReadCheckpoint(a)
	if err != nil || cp.Complete || !reflect.DeepEqual(cp.Outstanding, []string{"item_type:FutureBackgroundJob"}) {
		t.Fatalf("unknown item=%+v error=%v", cp, err)
	}
}

// This opt-in canary reads an explicitly supplied, completed disposable Codex
// recording. Normal test runs never inspect a user's session directory.
func TestCheckpointRecordedDisposableTranscript(t *testing.T) {
	raw := os.Getenv("AO_CUSTODY_TEST_CHECKPOINT_JSON")
	if raw == "" {
		t.Skip("no explicit disposable recording")
	}
	var a Attempt
	if err := json.Unmarshal([]byte(raw), &a); err != nil {
		t.Fatal(err)
	}
	cp, err := ReadCheckpoint(a)
	if err != nil || !cp.Complete || cp.Interrupted || len(cp.Outstanding) != 0 {
		t.Fatalf("checkpoint=%+v error=%v", cp, err)
	}
	t.Logf("completed turn=%s context_sha256=%s", cp.TurnID, cp.ContextSHA256)
}
