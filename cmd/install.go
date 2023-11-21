package cmd

import (
	"fmt"
	"io"
	"k8s.io/klog/v2"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

const (
	operatorInitDesc = `
 'install' command creates the Nineinfra platform along with all the dependencies.`
	operatorInitExample = `  kubectl nine install`
	DefaultNamespace    = "nineinfra"
)

var DefaultChartList = []string{
	"cloudnative-pg",
	"kyuubi-operator",
	"metastore-operator",
	"minio-directpv",
	"minio-operator",
	"nineinfra",
}

type operatorInstallCmd struct {
	out    io.Writer
	errOut io.Writer
	output bool
}

func newInstallCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	o := &operatorInstallCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "install",
		Short:   "Install the Nineinfra",
		Long:    operatorInitDesc,
		Example: operatorInitExample,
		Args:    cobra.MaximumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			err := o.run(out)
			if err != nil {
				klog.Warning(err)
				return err
			}
			return nil
		},
	}
	cmd = DisableHelp(cmd)
	return cmd
}

// run initializes local config and installs the Nineinfra to Kubernetes cluster.
func (o *operatorInstallCmd) run(writer io.Writer) error {

	if err := InitHelm(); err != nil {
		fmt.Printf("Error: %v \n", err)
		os.Exit(1)
	}

	path, _ := rootCmd.Flags().GetString(kubeconfig)

	parameters := []string{}
	if path != "" {
		parameters = append([]string{"--kubeconfig", path}, parameters...)
	}
	flags := strings.Join(parameters, " ")

	if err := CreateIfNotExist(DefaultNamespace, flags); err != nil {
		fmt.Printf("Error: %v \n", err)
		os.Exit(1)
	}

	for _, v := range DefaultChartList {
		err := HelmInstall(v, "", v, DefaultNamespace, flags)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
			os.Exit(1)
		}
	}

	return nil
}
