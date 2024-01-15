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
	deleteExample = `  kubectl nine delete c1 --namespace ns-c1`
)

type DeleteOptions struct {
	Name      string
	NS        string
	dangerous bool
	deletePVC bool
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
		Use:     "delete <NINECLUSTERNAME>",
		Short:   "Delete a NineCluster",
		Long:    deleteDesc,
		Example: deleteExample,
		Args: func(cmd *cobra.Command, args []string) error {
			return c.validate(args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if !c.deleteOpts.dangerous {
				fmt.Println("Please provide the i-know-it-is-dangerous flag to confirm deletion!")
				return fmt.Errorf("Aborting NineCluster deletion")
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
	f.BoolVar(&c.deleteOpts.dangerous, "i-know-it-is-dangerous", false, "confirm deletion")
	f.BoolVar(&c.deleteOpts.deletePVC, "delete-pvc", false, "delete the ninecluster's pvcs")
	cmd.MarkFlagRequired("namespace")

	return cmd
}

func (d *deleteCmd) validate(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("Not enough parameters!")
	}
	d.deleteOpts.Name = args[0]
	return ValidateClusterArgs("delete", args)
}

func constructMinioPVCLabel(name string) string {
	return DefaultMinioPVCLabelKey + "=" + name
}

func constructZookeeperPVCLabel(name string) string {
	return fmt.Sprintf("%s=%s,%s=%s", DefaultClusterLabelKey, name, DefaultAppLabelKey, "zookeeper")
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
	// delete minio pvc
	err = c.CoreV1().PersistentVolumeClaims(namespace).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: constructMinioPVCLabel(name)})
	if err != nil {
		return err
	}
	return nil
}

func deleteZookeeperPVC(name string, namespace string) error {
	if name == "" || namespace == "" {
		return nil
	}
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	c, err := GetKubeClient(path)
	if err != nil {
		return err
	}
	// delete minio pvc
	err = c.CoreV1().PersistentVolumeClaims(namespace).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: constructZookeeperPVCLabel(name)})
	if err != nil {
		return err
	}
	return nil
}

func deleteOlapPVC(name string, namespace string) error {
	if name == "" || namespace == "" {
		return nil
	}
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	c, err := GetKubeClient(path)
	if err != nil {
		return err
	}

	olapPvcLabel := DefaultOlapPVCLabelKey + "=" + name + DefaultDorisBENameSuffix
	err = c.CoreV1().PersistentVolumeClaims(namespace).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: olapPvcLabel})
	if err != nil {
		return err
	}
	olapPvcLabel = DefaultOlapPVCLabelKey + "=" + name + DefaultDorisFENameSuffix
	err = c.CoreV1().PersistentVolumeClaims(namespace).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: olapPvcLabel})
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

	if d.deleteOpts.deletePVC {
		if Ask("This is irreversible, are you sure you want to delete all pvcs of this NineCluster") {
			fmt.Println("All PVCs used by Nine will be deleted")
			err := deleteNineInfraPVC(d.deleteOpts.Name+DefaultNineSuffix, d.deleteOpts.NS)
			if err != nil {
				return err
			}
			err = deleteOlapPVC(d.deleteOpts.Name+DefaultNineSuffix, d.deleteOpts.NS)
			if err != nil {
				return err
			}
			err = deleteZookeeperPVC(d.deleteOpts.Name+DefaultNineSuffix, d.deleteOpts.NS)
			if err != nil {
				return err
			}
		} else {
			fmt.Println("All PVCs used by Nine will not be deleted")
		}
	}
	fmt.Println("NineCluster:" + d.deleteOpts.Name + " in namespace:" + d.deleteOpts.NS + " is deleted successfully!")
	fmt.Println("It may take a few minutes for it to be deleted completely")
	return nil
}
