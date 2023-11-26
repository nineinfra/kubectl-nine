package cmd

import (
	"context"
	"fmt"
	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/manifoldco/promptui"
	nineinfrav1alpha1 "github.com/nineinfra/nineinfra/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/duration"
)

func CheckNineClusterExist(name string, namespace string) (bool, *nineinfrav1alpha1.NineClusterList) {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	nc, err := GetNineInfraClient(path)
	if err != nil {
		return false, nil
	}

	if name != "" {
		c, err := nc.NineinfraV1alpha1().NineClusters(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err == nil {
			return true, &nineinfrav1alpha1.NineClusterList{
				Items: []nineinfrav1alpha1.NineCluster{
					*c,
				},
			}
		}
	} else {
		clist, err := nc.NineinfraV1alpha1().NineClusters(namespace).List(context.TODO(), metav1.ListOptions{})
		if err == nil {
			return true, clist
		}
	}
	return false, nil
}

// Ask user for Y/N input. Return true if response is "y"
func Ask(label string) bool {
	prompt := promptui.Prompt{
		Label:     label,
		IsConfirm: true,
		Default:   "n",
	}
	_, err := prompt.Run()
	return err == nil
}

func CheckStsIfReady(name string, namespace string) bool {
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

func IfPGReady(pg *cnpgv1.Cluster) bool {
	return pg.Status.ReadyInstances == pg.Spec.Instances
}

func CheckPGClusterIfReady(name string, namespace string) bool {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetPGOperatorClient(path)
	if err != nil {
		return false
	}
	pg, err := client.PostgresqlV1().Clusters(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return false
	}
	return IfPGReady(pg)
}

func CheckClusterIfReady(name string, namespace string) bool {
	for k, v := range NineClusterProjectWorkloadList {
		switch v {
		case "statefulset":
			if !CheckStsIfReady(name+k, namespace) {
				return false
			}
		case "cluster":
			if !CheckPGClusterIfReady(name+k, namespace) {
				return false
			}
		}
	}
	return true
}

func PrintClusterList(clusters *nineinfrav1alpha1.NineClusterList) {
	fmt.Printf("%-20s\t%-10s\t%-10s\t%-10s\t%-10s\n", "NAME", "DATAVOLUME", "READY", "NAMESPACE", "AGE")
	for _, cluster := range clusters.Items {
		ready := fmt.Sprintf("%t", CheckClusterIfReady(cluster.Name, cluster.Namespace))
		age := fmt.Sprintf("%s", duration.HumanDuration(metav1.Now().Sub(cluster.CreationTimestamp.Time)))
		datavolume := fmt.Sprintf("%d", cluster.Spec.DataVolume)
		fmt.Printf("%-20s\t%-10s\t%-10s\t%-10s\t%-10s\n", cluster.Name, datavolume, ready, cluster.Namespace, age)
	}
}
