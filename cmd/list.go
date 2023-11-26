package cmd

import (
	"context"
	"errors"
	"github.com/spf13/cobra"
	"io"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		Short: "List all nineclusters",
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
	PrintClusterList(clusters)

	return nil
}
