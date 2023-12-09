package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"k8s.io/klog/v2"
	"os"
	"strings"
)

const (
	operatorUninstallDesc = `
 'uninstall' command deletes the NineInfra platform along with all the dependencies.`
	operatorUninstallExample = `  kubectl nine uninstall`
)

type operatorUninstallCmd struct {
	out    io.Writer
	errOut io.Writer
	output bool
}

func newUninstallCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	o := &operatorUninstallCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "uninstall",
		Short:   "Uninstall the NineInfra",
		Long:    operatorUninstallDesc,
		Example: operatorUninstallExample,
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
	f := cmd.Flags()
	f.BoolVar(&DEBUG, "debug", false, "print debug infomation")
	return cmd
}

// run deletes the Nineinfra to Kubernetes cluster.
func (o *operatorUninstallCmd) run(writer io.Writer) error {
	exist, cl := CheckNineClusterExist("", "")
	if exist {
		fmt.Printf("Error: NineClusters Exists! Please delete these NineClusters firstly!\n")
		PrintClusterList(cl)
		os.Exit(1)
	}

	path, _ := rootCmd.Flags().GetString(kubeconfig)

	parameters := []string{}
	if path != "" {
		parameters = append(parameters, []string{"--kubeconfig", path}...)
	}
	flags := strings.Join(parameters, " ")
	for c, _ := range DefaultChartList {
		err := HelmUnInstall(c, "", DefaultNamespace, flags)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
			os.Exit(1)
		}
	}

	if err := RemoveHelmRepo(DefaultHelmRepo); err != nil {
		fmt.Printf("Error: %v \n", err)
		os.Exit(1)
	}

	//if err := DeleteIfExist(DefaultNamespace, "namespace", flags); err != nil {
	//	fmt.Printf("Error: %v \n", err)
	//	os.Exit(1)
	//}
	fmt.Println("NineInfra is uninstalled successfully!")
	fmt.Println("It may take a few minutes for it to be uninstalled completely")

	return nil
}
