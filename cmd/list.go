package cmd

import (
	"context"
	"errors"
	"fmt"
	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	ninefrav1alpha1 "github.com/nineinfra/nineinfra/api/v1alpha1"
	"github.com/spf13/cobra"
	"io"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/duration"
	"k8s.io/klog/v2"
)

const (
	listDesc = `'list' command lists all clusters managed by the Nineinfra`
)

type listCmd struct {
	out    io.Writer
	errOut io.Writer
}

func newClusterListCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &listCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all clusters",
		Long:  listDesc,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := c.validate(args); err != nil {
				return err
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
	return cmd
}

func (d *listCmd) validate(args []string) error {
	if len(args) != 0 {
		return errors.New("list command doesn't take any argument, try 'kubectl nine list'")
	}
	return nil
}

func (d *listCmd) run(_ []string) error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	nclient, err := GetNineInfraClient(path)
	if err != nil {
		return err
	}

	clusters, err := nclient.NineinfraV1alpha1().NineClusters("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	printClusterList(clusters)

	return nil
}

func checkStsIfReady(name string, namespace string) bool {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetKubeClient(path)
	if err != nil {
		return false
	}
	sts, err := client.AppsV1().StatefulSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return false
	}
	return sts.Status.ReadyReplicas == *sts.Spec.Replicas
}

func ifPGReady(pg *cnpgv1.Cluster) bool {
	return pg.Status.ReadyInstances == pg.Spec.Instances
}

func checkPGClusterIfReady(name string, namespace string) bool {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetPGOperatorClient(path)
	if err != nil {
		return false
	}
	pg, err := client.PostgresqlV1().Clusters(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return false
	}
	return ifPGReady(pg)
}

func checkClusterIfReady(name string, namespace string) bool {
	for k, v := range NineClusterProjectWorkloadList {
		switch v {
		case "statefulset":
			if !checkStsIfReady(name+k, namespace) {
				return false
			}
		case "cluster":
			if !checkPGClusterIfReady(name+k, namespace) {
				return false
			}
		}
	}
	return true
}

func printClusterList(clusters *ninefrav1alpha1.NineClusterList) {
	fmt.Printf("%-20s\t%-10s\t%-10s\t%-10s\t%-10s\n", "NAME", "DATAVOLUME", "READY", "NAMESPACE", "AGE")
	for _, cluster := range clusters.Items {
		ready := fmt.Sprintf("%t", checkClusterIfReady(cluster.Name, cluster.Namespace))
		age := fmt.Sprintf("%s", duration.HumanDuration(metav1.Now().Sub(cluster.CreationTimestamp.Time)))
		datavolume := fmt.Sprintf("%d", cluster.Spec.DataVolume)
		fmt.Printf("%-20s\t%-10s\t%-10s\t%-10s\t%-10s\n", cluster.Name, datavolume, ready, cluster.Namespace, age)
	}
}
