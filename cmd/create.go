package cmd

import (
	"context"
	"errors"
	"fmt"
	nineinfrav1alpha1 "github.com/nineinfra/nineinfra/api/v1alpha1"
	"github.com/spf13/cobra"
	"io"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
)

const (
	createDesc    = `'create' command create a NineCluster by the NineInfra`
	createExample = ` kubectl nine create c1 --datavolume 16 --namespace c1-ns`
)

var (
	olapsSupported = "doris"
)

// ClusterOptions encapsulates the CLI options for a NineCluster
type ClusterOptions struct {
	Name       string
	NS         string
	DataVolume int
	Olap       string
}

type createCmd struct {
	out         io.Writer
	errOut      io.Writer
	output      bool
	clusterOpts ClusterOptions
}

// Validate NineCluster Options
func (t ClusterOptions) Validate() error {
	if t.DataVolume <= 0 {
		return errors.New("--datavolume flag is required")
	}
	if t.Olap != "" && !strings.Contains(olapsSupported, t.Olap) {
		return errors.New(fmt.Sprintf("invalid olap:%s,support [%s]", t.Olap, olapsSupported))
	}
	return nil
}

func newClusterCreateCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &createCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "create <NINECLUSTERNAME> --datavolume <SIZE>",
		Short:   "Create a NineCluster",
		Long:    createDesc,
		Example: createExample,
		Args: func(cmd *cobra.Command, args []string) error {
			return c.validate(args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := c.run(args)
			if err != nil {
				return err
			}
			return nil
		},
	}
	cmd = DisableHelp(cmd)
	f := cmd.Flags()
	f.IntVarP(&c.clusterOpts.DataVolume, "data-volume", "v", 32, "total raw data volumes of the ninecluster,default uint Gi, e.g. 16")
	f.StringVarP(&c.clusterOpts.Olap, "olap", "a", "", fmt.Sprintf("add olap to the ninecluster,support [%s]", olapsSupported))
	f.StringVarP(&c.clusterOpts.NS, "namespace", "n", "", "k8s namespace for this ninecluster")
	return cmd
}

func (c *createCmd) validate(args []string) error {
	if args == nil {
		return errors.New("create command requires specifying the ninecluster name as an argument, e.g. 'kubectl nine create c1'")
	}
	if len(args) != 1 {
		return errors.New("create command requires specifying the ninecluster name as an argument, e.g. 'kubectl nine create c1'")
	}
	if args[0] == "" {
		return errors.New("create command requires specifying the ninecluster name as an argument, e.g. 'kubectl nine create c1'")
	}

	if err := CheckValidClusterName(args[0]); err != nil {
		return err
	}
	c.clusterOpts.Name = args[0]
	if c.clusterOpts.NS == "" {
		return errors.New("--namespace flag is required")
	}
	return c.clusterOpts.Validate()
}

// run initializes local config and creates a NineCluster to Kubernetes cluster.
func (c *createCmd) run(_ []string) error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	nc, err := GetNineInfraClient(path)
	if err != nil {
		return err
	}
	var features = map[string]string{}
	if c.clusterOpts.Olap != "" {
		features[FeaturesOlapKey] = c.clusterOpts.Olap
	}
	desiredNineCluster := &nineinfrav1alpha1.NineCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.clusterOpts.Name,
			Namespace: c.clusterOpts.NS,
		},
		Spec: nineinfrav1alpha1.NineClusterSpec{
			DataVolume: c.clusterOpts.DataVolume,
			Features:   features,
		},
	}

	exists, _ := CheckNineClusterExist(c.clusterOpts.Name, c.clusterOpts.NS)
	if exists {
		return errors.New("NineCluster:" + c.clusterOpts.Name + " already exists in namespace:" + c.clusterOpts.NS + "!")
	}

	_, err = nc.NineinfraV1alpha1().NineClusters(c.clusterOpts.NS).Create(context.TODO(), desiredNineCluster, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	fmt.Println("NineCluster:" + c.clusterOpts.Name + " in namespace:" + c.clusterOpts.NS + " is created successfully!")
	fmt.Println("It may take a few minutes for it to be ready")
	fmt.Println("You can check its status using the following command：")
	fmt.Println("kubectl nine show " + c.clusterOpts.Name + " -n " + c.clusterOpts.NS)

	return nil
}
