package custody

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Checkpoint describes the durable provider ledger, not process cessation.
// The coordinator must additionally drain hooks/operations and verify every
// retained process identity before publishing custody.
type Checkpoint struct {
	TurnID        string
	ContextSHA256 string
	Outstanding   []string
	Complete      bool
	Interrupted   bool
}

const maxCheckpointTranscript = 32 << 20

var checkpointCell = regexp.MustCompile(`^Script running with cell ID ([A-Za-z0-9_-]+)(?:\r?\n|$)`)

// Only an exact single polling expression can settle an interrupted wrapper
// from a later completion of its original process. Arbitrary JavaScript is not
// evaluated or treated as a declaration of the children it launched.
var checkpointPoll = regexp.MustCompile(`^\s*text\(await tools\.write_stdin\(\{\s*session_id\s*:\s*([1-9][0-9]*)\s*,\s*chars\s*:\s*""\s*,\s*yield_time_ms\s*:\s*[0-9]+\s*\}\)\);?\s*$`)

type checkpointCall struct {
	name      string
	poll      string
	cell      string
	returned  bool
	unsettled bool
}

type checkpointLedger struct {
	attempt            Attempt
	result             Checkpoint
	meta               bool
	completed          string
	calls              map[string]*checkpointCall
	processes          map[string]bool
	completedProcesses map[string]bool
	commandIDs         map[string]bool
	turns              map[string]bool
	cells              map[string]bool
	unknown            map[string]bool
}

// ReadCheckpoint reads a stable, bounded local JSONL snapshot. A quiet TUI,
// turn_aborted, failed lookup, or model's handoff wording never settles a child.
func ReadCheckpoint(a Attempt) (Checkpoint, error) {
	l := checkpointLedger{attempt: a, calls: map[string]*checkpointCall{}, processes: map[string]bool{}, completedProcesses: map[string]bool{}, commandIDs: map[string]bool{}, turns: map[string]bool{}, cells: map[string]bool{}, unknown: map[string]bool{}}
	if a.ProviderID == "" || a.Workspace == "" || !filepath.IsAbs(a.Transcript) || !filepath.IsAbs(a.Workspace) {
		return l.result, fmt.Errorf("%w: incomplete transcript identity", ErrUnknown)
	}
	info, err := os.Lstat(a.Transcript)
	if err != nil || !info.Mode().IsRegular() || info.Size() == 0 || info.Size() > maxCheckpointTranscript {
		return l.result, fmt.Errorf("%w: transcript is unavailable or exceeds bounded capture", ErrUnknown)
	}
	f, err := os.Open(a.Transcript)
	if err != nil {
		return l.result, err
	}
	defer func() { _ = f.Close() }()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return l.result, ErrUnknown
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxCheckpointTranscript+1))
	if err != nil {
		return l.result, err
	}
	after, err := f.Stat()
	current, currentErr := os.Lstat(a.Transcript)
	if err != nil || currentErr != nil || !os.SameFile(opened, current) || !current.Mode().IsRegular() || len(raw) > maxCheckpointTranscript || int64(len(raw)) != opened.Size() || after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) {
		return l.result, fmt.Errorf("%w: transcript changed during capture", ErrUnknown)
	}
	sum := sha256.Sum256(raw)
	l.result.ContextSHA256 = hex.EncodeToString(sum[:])
	if raw[len(raw)-1] != '\n' {
		return l.result, fmt.Errorf("%w: incomplete transcript record", ErrUnknown)
	}
	for _, line := range bytes.Split(raw[:len(raw)-1], []byte{'\n'}) {
		if len(line) == 0 {
			return l.result, fmt.Errorf("%w: empty transcript record", ErrUnknown)
		}
		var record map[string]json.RawMessage
		if err := checkpointUniqueJSON(line); err != nil {
			return l.result, err
		}
		if err := json.Unmarshal(line, &record); err != nil || record == nil {
			return l.result, fmt.Errorf("%w: invalid transcript JSON", ErrUnknown)
		}
		if err := l.record(record); err != nil {
			return l.result, err
		}
	}
	if !l.meta {
		return l.result, fmt.Errorf("%w: missing transcript session identity", ErrUnknown)
	}
	for id, call := range l.calls {
		if !call.returned || call.unsettled {
			if call.returned && call.poll != "" && l.completedProcesses[call.poll] {
				continue
			}
			l.unknown["tool:"+id] = true
		}
	}
	for id := range l.processes {
		if !l.completedProcesses[id] {
			l.unknown["process:"+id] = true
		}
	}
	for id := range l.cells {
		l.unknown["cell:"+id] = true
	}
	for id := range l.unknown {
		l.result.Outstanding = append(l.result.Outstanding, id)
	}
	sort.Strings(l.result.Outstanding)
	if l.result.Outstanding == nil {
		l.result.Outstanding = []string{}
	}
	l.result.Complete = l.result.TurnID != "" && l.result.TurnID == l.completed && l.result.TurnID == a.TurnID && l.result.TurnID == a.StopHookTurn && len(l.result.Outstanding) == 0
	return l.result, nil
}

func checkpointString(raw json.RawMessage) string {
	var s string
	_ = json.Unmarshal(raw, &s)
	return s
}
func checkpointObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var out map[string]json.RawMessage
	err := json.Unmarshal(raw, &out)
	if err != nil || out == nil {
		return nil, ErrUnknown
	}
	return out, nil
}
func (l *checkpointLedger) record(r map[string]json.RawMessage) error {
	kind := checkpointString(r["type"])
	p, err := checkpointObject(r["payload"])
	if err != nil {
		return err
	}
	switch kind {
	case "session_meta":
		if l.meta || checkpointString(p["id"]) != l.attempt.ProviderID {
			return ErrConflict
		}
		if !filepath.IsAbs(checkpointString(p["cwd"])) {
			return ErrConflict
		}
		cwd, err := filepath.EvalSymlinks(checkpointString(p["cwd"]))
		wanted, wantedErr := filepath.EvalSymlinks(l.attempt.Workspace)
		if err != nil || wantedErr != nil || cwd != wanted {
			return ErrConflict
		}
		l.meta = true
	case "event_msg":
		return l.event(p)
	case "response_item":
		return l.response(p)
	case "turn_context", "world_state", "token_usage_record", "compacted":
		// These records carry context but do not settle execution operations.
	default:
		return fmt.Errorf("%w: unknown transcript record %q", ErrUnknown, kind)
	}
	return nil
}
func (l *checkpointLedger) event(p map[string]json.RawMessage) error {
	kind := checkpointString(p["type"])
	switch kind {
	case "task_started":
		turn := checkpointString(p["turn_id"])
		if turn == "" || l.turns[turn] {
			return ErrUnknown
		}
		l.turns[turn] = true
		l.result.TurnID = turn
		l.result.Interrupted = false
		l.completed = ""
	case "task_complete":
		turn := checkpointString(p["turn_id"])
		if turn == "" || turn != l.result.TurnID {
			return ErrConflict
		}
		l.completed = turn
		l.result.Interrupted = false
	case "turn_aborted", "task_aborted":
		if checkpointString(p["turn_id"]) != l.result.TurnID {
			return ErrConflict
		}
		l.completed = ""
		l.result.Interrupted = true
	case "item_started", "item_completed":
		if thread := checkpointString(p["thread_id"]); thread != "" && thread != l.attempt.ProviderID {
			return ErrConflict
		}
		item, err := checkpointObject(p["item"])
		if err != nil {
			return err
		}
		typ := checkpointString(item["type"])
		if typ == "CommandExecution" {
			if !l.turns[checkpointString(p["turn_id"])] {
				return ErrConflict
			}
			commandID := checkpointString(item["id"])
			if commandID == "" {
				return ErrUnknown
			}
			if kind == "item_completed" {
				if l.commandIDs[commandID] {
					return ErrConflict
				}
				l.commandIDs[commandID] = true
			}
			id := checkpointString(item["process_id"])
			if id == "" {
				l.unknown["command_without_process_id:"+checkpointString(item["id"])] = true
				return nil
			}
			l.processes[id] = true
			var code int
			codeRaw, hasCode := item["exit_code"]
			status := checkpointString(item["status"])
			if kind == "item_completed" && hasCode && string(codeRaw) != "null" && json.Unmarshal(codeRaw, &code) == nil && (status == "completed" || status == "failed") {
				l.completedProcesses[id] = true
			}
		} else if typ == "CollabAgentToolCall" || typ == "DynamicToolCall" || typ == "McpToolCall" {
			// Tool-return accounting is separate; an explicitly incomplete
			// external/child-agent operation must not disappear at Stop.
			if kind == "item_started" {
				l.unknown["item:"+checkpointString(item["id"])] = true
			}
			if kind == "item_completed" {
				delete(l.unknown, "item:"+checkpointString(item["id"]))
			}
		} else {
			switch typ {
			case "UserMessage", "AgentMessage", "Reasoning", "Plan", "WebSearch", "ImageView", "ImageGeneration", "ContextCompaction":
			default:
				l.unknown["item_type:"+typ] = true
			}
		}
	case "token_count", "agent_message", "agent_reasoning", "user_message", "thread_settings_applied", "context_compacted", "warning", "deprecation_notice":
	default:
		// New execution event semantics require a reader update, not silence
		// or a newer task_complete that erases uncertain earlier operations.
		l.unknown["event:"+kind] = true
	}
	return nil
}
func (l *checkpointLedger) response(p map[string]json.RawMessage) error {
	switch checkpointString(p["type"]) {
	case "custom_tool_call", "function_call":
		id := checkpointString(p["call_id"])
		if id == "" || l.calls[id] != nil {
			return ErrConflict
		}
		c := &checkpointCall{name: checkpointString(p["name"])}
		if l.completed != "" {
			l.unknown["execution_after_complete:"+id] = true
		}
		if c.name == "spawn_agent" || c.name == "create_thread" || c.name == "fork_thread" {
			c.unsettled = true
		}
		input := checkpointString(p["input"])
		if input == "" {
			input = checkpointString(p["arguments"])
		}
		if c.name == "exec" {
			if match := checkpointPoll.FindStringSubmatch(input); match != nil {
				c.poll = match[1]
			}
		}
		if c.name == "wait" {
			var args struct {
				CellID string `json:"cell_id"`
			}
			if json.Unmarshal([]byte(input), &args) != nil || args.CellID == "" {
				c.unsettled = true
			} else {
				c.cell = args.CellID
			}
		}
		l.calls[id] = c
	case "custom_tool_call_output", "function_call_output":
		id := checkpointString(p["call_id"])
		c := l.calls[id]
		if c == nil || c.returned {
			return ErrConflict
		}
		c.returned = true
		return l.output(id, c, p["output"])
	case "message", "reasoning":
	default:
		l.unknown["response:"+checkpointString(p["type"])] = true
	}
	return nil
}
func (l *checkpointLedger) output(id string, c *checkpointCall, raw json.RawMessage) error {
	var text string
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &text) == nil {
		blocks = append(blocks, struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}{"input_text", text})
	} else if json.Unmarshal(raw, &blocks) != nil {
		c.unsettled = true
		return nil
	}
	if len(blocks) == 0 {
		c.unsettled = true
		return nil
	}
	header := blocks[0].Text
	if c.name == "exec" || c.name == "wait" {
		if strings.HasPrefix(header, "Script completed\n") {
			if c.name == "wait" {
				if c.cell == "" || !l.cells[c.cell] {
					c.unsettled = true
				} else {
					delete(l.cells, c.cell)
				}
			}
		} else if m := checkpointCell.FindStringSubmatch(header); m != nil {
			if c.name == "wait" && c.cell != m[1] {
				c.unsettled = true
			}
			l.cells[m[1]] = true
		} else {
			c.unsettled = true
		}
	}
	for _, block := range blocks {
		if block.Type != "input_text" && block.Type != "text" {
			continue
		}
		// Parse only a tool wrapper's direct JSON object, never nested stdout:
		// printing a fake exit_code in a command cannot settle a process.
		var value map[string]json.RawMessage
		if json.Unmarshal([]byte(block.Text), &value) != nil || value == nil {
			continue
		}
		if session, ok := value["session_id"]; ok {
			var n json.Number
			if json.Unmarshal(session, &n) != nil {
				l.unknown["child_output:"+id] = true
				continue
			}
			pid, err := strconv.ParseInt(string(n), 10, 64)
			if err != nil || pid < 1 {
				l.unknown["child_output:"+id] = true
				continue
			}
			if l.completedProcesses[string(n)] && c.poll != string(n) {
				delete(l.completedProcesses, string(n))
			}
			l.processes[string(n)] = true
		}
		if cell, ok := value["cell_id"]; ok {
			sid := checkpointString(cell)
			if sid == "" {
				l.unknown["child_output:"+id] = true
			} else {
				l.cells[sid] = true
			}
		}
		for _, key := range []string{"agent_id", "threadId", "clientThreadId", "operationId"} {
			if _, ok := value[key]; ok {
				l.unknown["child_output:"+id] = true
			}
		}
	}
	return nil
}

// A duplicate control field is ambiguous even when encoding/json would choose
// its last value. Reject it before deriving completed execution facts.
func checkpointUniqueJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 100 {
			return ErrUnknown
		}
		token, err := decoder.Token()
		if err != nil {
			return ErrUnknown
		}
		delimiter, container := token.(json.Delim)
		if !container {
			return nil
		}
		switch delimiter {
		case '{':
			seen := map[string]bool{}
			for decoder.More() {
				key, err := decoder.Token()
				name, ok := key.(string)
				if err != nil || !ok || seen[name] {
					return ErrUnknown
				}
				seen[name] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		default:
			return ErrUnknown
		}
		if _, err := decoder.Token(); err != nil {
			return ErrUnknown
		}
		return nil
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return ErrUnknown
	}
	return nil
}
