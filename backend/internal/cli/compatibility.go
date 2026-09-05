package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/aoagents/agent-orchestrator/backend/internal/bootguard"
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/daemon"
)

// This offline diagnostic reads only the compatibility marker, never product
// storage or HTTP. Installers must supply their own held transition ownership.
func newCompatibilityCommand() *cobra.Command {
	var jsonOutput bool
	cmd := &cobra.Command{
		Use: "compatibility", Short: "Inspect binary/data compatibility without starting or changing AO", Args: noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			status, inspectErr := bootguard.Inspect(cfg.DataDir)
			if jsonOutput {
				if err := writeJSON(cmd.OutOrStdout(), status); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s: supported protocol %d, marker %s (inspection only)\n", status.State, status.SupportedProtocol, status.MarkerPath); err != nil {
					return err
				}
			}
			return inspectErr
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output compatibility inspection as JSON")
	return cmd
}

func newPrepareStartPausedCommand() *cobra.Command {
	var jsonOutput bool
	cmd := &cobra.Command{Use: "prepare-start-paused", Short: "Persist admission pauses offline before starting AO", Args: noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			proof, err := daemon.PrepareStartPaused(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			if jsonOutput {
				return writeJSON(cmd.OutOrStdout(), proof)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Prepared %d project admission pauses; existing sessions may still be running.\n", len(proof.ProjectIDs))
			return err
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output offline preparation proof as JSON")
	return cmd
}
