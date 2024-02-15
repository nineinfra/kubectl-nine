package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	out       io.Writer
	errOut    io.Writer
	output    bool
	deleteCrd bool
}

func deleteCrd(crd string) error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	c, err := GetKubeDynamicClient(path)
	if err != nil {
		return err
	}
	// delete crd
	crdResource := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	err = c.Resource(crdResource).Delete(context.TODO(), crd, metav1.DeleteOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	return nil
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
	f.BoolVar(&DEBUG, "debug", false, "print debug infomation")
	f.BoolVar(&o.deleteCrd, "delete-crd", false, "delete crd")
	return cmd
}

// run deletes the Nineinfra to Kubernetes cluster.
func (o *operatorUninstallCmd) run() error {
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
	for c := range DefaultChartList {
		err := HelmUnInstall(c, DefaultNamespace, flags)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
			return err
		}
	}

	if err := RemoveHelmRepo(DefaultHelmRepo); err != nil {
		fmt.Printf("Error: %v \n", err)
		return err
	}

	//if err := DeleteIfExist(DefaultNamespace, "namespace", flags); err != nil {
	//	fmt.Printf("Error: %v \n", err)
	//	os.Exit(1)
	//}

	if o.deleteCrd {
		for _, crd := range NineInfraCrdList {
			err := deleteCrd(crd)
			if err != nil {
				fmt.Printf("Error: %v \n", err)
				return err
			}
		}
	}

	fmt.Println("NineInfra is uninstalled successfully!")
	fmt.Println("It may take a few minutes for it to be uninstalled completely")

	return nil
}
