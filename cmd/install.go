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
	operatorInstallDesc = `
 'install' command creates the NineInfra platform along with all the dependencies.`
	operatorInstallExample = `  kubectl nine install`
)

type operatorInstallCmd struct {
	out       io.Writer
	errOut    io.Writer
	output    bool
	chartPath string
}

func newInstallCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	o := &operatorInstallCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "install",
		Short:   "Install the NineInfra",
		Long:    operatorInstallDesc,
		Example: operatorInstallExample,
		Args:    cobra.MaximumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			err := o.run()
			if err != nil {
				klog.Warning(err)
				return err
			}
			return nil
		},
	}
	cmd = DisableHelp(cmd)
	f := cmd.Flags()
	f.BoolVar(&DEBUG, "debug", false, "print debug information")
	f.StringVarP(&o.chartPath, "chart-path", "p", "", "local path of the charts")
	return cmd
}

// run initializes local config and installs the Nineinfra to Kubernetes cluster.
func (o *operatorInstallCmd) run() error {

	path, _ := rootCmd.Flags().GetString(kubeconfig)

	var parameters []string
	if path != "" {
		parameters = append(parameters, []string{"--kubeconfig", path}...)
	}
	flags := strings.Join(parameters, " ")

	if err := InitHelm(); err != nil {
		fmt.Printf("Error: %v \n", err)
		os.Exit(1)
	}

	if err := CreateIfNotExist(DefaultNamespace, "namespace", flags); err != nil {
		fmt.Printf("Error: %v \n", err)
		os.Exit(1)
	}

	for c, v := range DefaultChartList {
		err := HelmInstall(c, "", o.chartPath, c, v, DefaultNamespace, flags)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
			os.Exit(1)
		}
	}

	fmt.Println("NineInfra is installed successfully!")
	fmt.Println("It may take a few minutes for it to be ready")
	fmt.Println("You can check its status using the following command")
	fmt.Println("kubectl nine status")

	return nil
}
