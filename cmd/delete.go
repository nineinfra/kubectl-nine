package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const (
	deleteDesc = `
'delete' command delete the NineCluster along with all the dependencies.`
	deleteExample = `  kubectl nine delete c1`
)

// ClusterOptions encapsulates the CLI options for a NineCluster
type DeleteOptions struct {
	Name      string
	NS        string
	force     bool
	dangerous bool
	retainPVC bool
}

type deleteCmd struct {
	out        io.Writer
	errOut     io.Writer
	output     bool
	deleteOpts DeleteOptions
}

func newClusterDeleteCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &deleteCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "delete <NINECLUSTERNAME> --namespace <NINECLUSTERNS>",
		Short:   "Delete a NineCluster",
		Long:    deleteDesc,
		Example: deleteExample,
		Args: func(cmd *cobra.Command, args []string) error {
			return c.validate(args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if !c.deleteOpts.force {
				if !Ask(fmt.Sprintf("This will delete the NineCluster %s and ALL its data. Do you want to proceed", args[0])) {
					return fmt.Errorf(Bold("Aborting NineCluster deletion"))
				}
			}
			if !c.deleteOpts.dangerous {
				if !Ask("Please provide the dangerous flag to confirm deletion") {
					return fmt.Errorf(Bold("Aborting NineCluster deletion"))
				}
			}

			err := c.run(args)
			if err != nil {
				klog.Warning(err)
				return err
			}
			return nil
		},
	}
	cmd = DisableHelp(cmd)
	f := cmd.Flags()
	f.StringVarP(&c.deleteOpts.NS, "namespace", "n", "", "namespace scope for this request")
	f.BoolVarP(&c.deleteOpts.force, "force", "f", false, "allow without confirmation")
	f.BoolVarP(&c.deleteOpts.dangerous, "dangerous", "d", false, "confirm deletion")
	f.BoolVarP(&c.deleteOpts.retainPVC, "retain-pvc", "r", true, "retain ninecluster pvcs")
	cmd.MarkFlagRequired("namespace")

	return cmd
}

func (d *deleteCmd) validate(args []string) error {
	d.deleteOpts.Name = args[0]
	return ValidateClusterArgs("delete", args)
}

func constructPVCLabel(name string) string {
	return DefaultPVCLabelKey + "=" + name
}

func deleteNineInfraPVC(name string, namespace string) error {
	if name == "" || namespace == "" {
		return nil
	}
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	c, err := GetKubeClient(path)
	if err != nil {
		return err
	}

	err = c.CoreV1().PersistentVolumeClaims(namespace).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: constructPVCLabel(name)})
	if err != nil {
		return err
	}
	return nil
}

func (d *deleteCmd) run(args []string) error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	nc, err := GetNineInfraClient(path)
	if err != nil {
		return err
	}

	err = nc.NineinfraV1alpha1().NineClusters(d.deleteOpts.NS).Delete(context.TODO(), args[0], metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	if !d.deleteOpts.retainPVC {
		err := deleteNineInfraPVC(d.deleteOpts.Name, d.deleteOpts.NS)
		if err != nil {
			return err
		}
	}
	return nil
}
