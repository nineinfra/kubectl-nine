package cmd

import (
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	"os"
	"os/exec"
)

// DisableHelp disables the help command
func DisableHelp(cmd *cobra.Command) *cobra.Command {
	cmd.SetHelpCommand(&cobra.Command{
		Use:    "no-help",
		Hidden: true,
	})
	return cmd
}

func CreateIfNotExist(resource string, flags string) error {
	cmd := exec.Command("kubectl", "create", resource, flags)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return nil
}
