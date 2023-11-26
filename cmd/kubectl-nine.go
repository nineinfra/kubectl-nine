package cmd

import (
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"log"

	"github.com/spf13/cobra"
	// Workaround for auth import issues refer https://github.com/minio/operator/issues/283
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

const (
	nineDesc   = `Manage the Nineinfra and nineclusters on k8s`
	kubeconfig = "kubeconfig"
)

var (
	confPath string
	rootCmd  = &cobra.Command{
		Use:          "nine",
		Long:         nineDesc,
		SilenceUsage: true,
	}
)

func init() {
	rootCmd.PersistentFlags().StringVar(&confPath, kubeconfig, "", "Custom kubeconfig path")

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

// New creates a new root command for kubectl-nine
func New(_ genericiooptions.IOStreams) *cobra.Command {
	rootCmd = DisableHelp(rootCmd)
	cobra.EnableCommandSorting = false
	rootCmd.AddCommand(newInstallCmd(rootCmd.OutOrStdout(), rootCmd.ErrOrStderr()))
	rootCmd.AddCommand(newUninstallCmd(rootCmd.OutOrStdout(), rootCmd.ErrOrStderr()))
	rootCmd.AddCommand(newClusterCreateCmd(rootCmd.OutOrStdout(), rootCmd.ErrOrStderr()))
	rootCmd.AddCommand(newClusterDeleteCmd(rootCmd.OutOrStdout(), rootCmd.ErrOrStderr()))
	rootCmd.AddCommand(newClusterListCmd(rootCmd.OutOrStdout(), rootCmd.ErrOrStderr()))
	rootCmd.AddCommand(newNineStatusCmd(rootCmd.OutOrStdout(), rootCmd.ErrOrStderr()))
	return rootCmd
}
