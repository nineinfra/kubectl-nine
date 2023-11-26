package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	v1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/duration"
	"k8s.io/klog/v2"
)

const (
	statusDesc = `'status' command displays the NineInfra status information`
)

type statusCmd struct {
	out        io.Writer
	errOut     io.Writer
	ns         string
	yamlOutput bool
	jsonOutput bool
}

func newNineStatusCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &statusCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "status",
		Short:   "Display nineinfra status",
		Long:    statusDesc,
		Example: `  kubectl nine status`,
		Args:    cobra.MaximumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
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
	f.BoolVarP(&c.yamlOutput, "yaml", "y", false, "yaml output")
	f.BoolVarP(&c.jsonOutput, "json", "j", false, "json output")
	return cmd
}

func (d *statusCmd) run(args []string) error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetKubeClient(path)
	if err != nil {
		return err
	}
	deploys, err := client.AppsV1().Deployments(DefaultNamespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	printNineInfra(deploys)
	return nil
}

func printNineInfra(deploys *v1.DeploymentList) {
	fmt.Printf("%-20s\t%-10s\t%-10s\n", "NAME", "READY", "AGE")
	for _, deploy := range deploys.Items {
		ready := fmt.Sprintf("%t", deploy.Status.ReadyReplicas == *deploy.Spec.Replicas)
		age := fmt.Sprintf("%s", duration.HumanDuration(metav1.Now().Sub(deploy.CreationTimestamp.Time)))
		fmt.Printf("%-20s\t%-10s\t%-10s\n", NineInfraDeploymentAlias[deploy.Name], ready, age)
	}
}
