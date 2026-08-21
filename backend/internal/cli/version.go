package cli

import (
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/buildinfo"
	"github.com/spf13/cobra"
)

// VersionString renders the build metadata as "<version> commit <c> built <d>",
// omitting the commit/date parts when they are unset.
func VersionString() string {
	return buildinfo.VersionString()
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), VersionString())
			return err
		},
	}
}
