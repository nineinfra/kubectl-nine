package cmd

import (
	"context"
	"github.com/spf13/cobra"
	"io"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const (
	showDesc    = `'show' command displays the NineCluster's projects status`
	showExample = ` kubectl nine show c1 --namespace c1-ns`
)

type showCmd struct {
	out    io.Writer
	errOut io.Writer
	name   string
	ns     string
}

func newClusterShowCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &showCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "show <NINECLUSTERNAME> --namespace <NINECLUSTERNS>",
		Short:   "Display the NineCluster's projects status",
		Long:    showDesc,
		Example: showExample,
		Args: func(cmd *cobra.Command, args []string) error {
			return c.validate(args)
		},
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
	f.StringVarP(&c.ns, "namespace", "n", "", "k8s namespace for this ninecluster")
	return cmd
}

func (s *showCmd) validate(args []string) error {
	s.name = args[0]
	return ValidateClusterArgs("show", args)
}

func (s *showCmd) run(_ []string) error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	nclient, err := GetNineInfraClient(path)
	if err != nil {
		return err
	}

	ninecluster, err := nclient.NineinfraV1alpha1().NineClusters(s.ns).Get(context.TODO(), s.name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	PrintNineCluster(ninecluster)

	return nil
}
