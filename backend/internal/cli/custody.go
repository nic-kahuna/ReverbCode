package cli

import (
	"encoding/json"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/daemon"
	"github.com/spf13/cobra"
)

// These commands retain their JSON body unchanged across the thin client. The
// daemon owns strict schema/identity validation; CLI never opens custody state.
func nativeJSONCommand(c *commandContext, name, summary, path string) *cobra.Command {
	var request string
	cmd := &cobra.Command{Use: name, Short: summary, Args: noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !json.Valid([]byte(request)) {
				return usageError{fmt.Errorf("--request-json requires one JSON object")}
			}
			var out json.RawMessage
			if err := c.postJSON(cmd.Context(), path, json.RawMessage(request), &out); err != nil {
				return err
			}
			return writeJSON(cmd.OutOrStdout(), out)
		},
	}
	cmd.Flags().StringVar(&request, "request-json", "", "Exact versioned native request JSON")
	return cmd
}
func newAdmissionCommand(c *commandContext) *cobra.Command {
	cmd := &cobra.Command{Use: "admission", Short: "Inspect mandatory managed execution admission"}
	cmd.AddCommand(nativeJSONCommand(c, "evidence", "Read exact native attempt evidence without starting AO", "admission/evidence"))
	return cmd
}
func newCustodyCommand(c *commandContext) *cobra.Command {
	cmd := &cobra.Command{Use: "custody", Short: "Inspect preserving managed generation custody"}
	cmd.AddCommand(nativeJSONCommand(c, "certificate", "Read and verify immutable preservation evidence", "custody/certificate"))
	for _, operation := range []string{"request", "status", "checkpoint", "verify", "handback"} {
		cmd.AddCommand(nativeJSONCommand(c, operation, "Coordinate exact preserving native custody", "custody/"+operation))
	}
	return cmd
}

func newPrepareCustodyCommand() *cobra.Command {
	var projects []string
	var outputJSON bool
	cmd := &cobra.Command{Use: "prepare-custody", Short: "Activate managed custody offline for exact already-paused projects", Args: noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if len(projects) == 0 {
				return usageError{fmt.Errorf("at least one --project is required")}
			}
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			out, err := daemon.PrepareCustody(cmd.Context(), cfg, projects)
			if err != nil {
				return err
			}
			if outputJSON {
				return writeJSON(cmd.OutOrStdout(), out)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Activated protocol 2 custody for %d paused projects; no mutation lanes started.\n", len(out.ProjectIDs))
			return err
		},
	}
	cmd.Flags().StringArrayVar(&projects, "project", nil, "Exact already-paused project id (repeatable)")
	cmd.Flags().BoolVar(&outputJSON, "json", false, "Output exact activation evidence as JSON")
	return cmd
}
