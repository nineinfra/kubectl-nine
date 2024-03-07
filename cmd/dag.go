package cmd

import (
	"fmt"
	nineinfrav1alpha1 "github.com/nineinfra/nineinfra/api/v1alpha1"
	"github.com/spf13/cobra"
	"io"
	"path/filepath"
	"strings"
)

const (
	dagDesc    = `'etl' command help you to complete the DAG-related operations with the NineCluster.`
	dagExample = `1. Upload dags to the NineCluster
   $ kubectl nine dag --command=upload --namespace=ns

2. Clear dags from the NineCluster from a namespace
   $ kubectl nine dag --command=clear --namespace=ns`
)

var (
	dagSubCommandList = "upload,clear,list"
)

type dagCmd struct {
	out        io.Writer
	errOut     io.Writer
	subCommand string
	ns         string
	nineName   string
	dagsPath   string
}

func newDagCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &dagCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "dag",
		Short:   "Helps you to complete the DAG-related operations with the NineCluster.",
		Long:    dagDesc,
		Example: dagExample,
		Args: func(cmd *cobra.Command, args []string) error {
			return c.validate(args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := c.run()
			if err != nil {
				return err
			}
			return nil
		},
	}
	cmd = DisableHelp(cmd)
	f := cmd.Flags()
	f.StringVarP(&c.subCommand, "command", "c", "", fmt.Sprintf("command for tools,%s are supported now", dagSubCommandList))
	f.StringVar(&c.dagsPath, "dags-path", "", "local path of the airflow dags")
	f.StringVarP(&c.ns, "namespace", "n", "", "k8s namespace for the NineCluster")
	f.BoolVar(&DEBUG, "debug", false, "print debug information")
	return cmd
}

func (o *dagCmd) validate(args []string) error {
	if !strings.Contains(dagSubCommandList, o.subCommand) {
		return fmt.Errorf("unsupported subcommand %s, only %s supported", o.subCommand, dagSubCommandList)
	}

	return nil
}

func (o *dagCmd) uploadFileToAirflow(filename string, cluster *nineinfrav1alpha1.NineCluster) error {
	podNames, err := GetAirflowPodNames(cluster.Name, "scheduler", cluster.Namespace)
	if err != nil {
		return err
	}
	_, _, err = runCommand("kubectl", "cp", filename, fmt.Sprintf("%s:%s/%s", podNames[0], DefaultAirflowDagsPath, filepath.Base(filename)), "-n", cluster.Namespace)
	if err != nil {
		return err
	}
	return nil
}

func (o *dagCmd) list(parameters []string) error {
	listClusters, err := GetNineCLusters(o.ns)
	if err != nil {
		return err
	}
	o.nineName = listClusters.Items[0].Name
	podNames, err := GetAirflowPodNames(o.nineName, "scheduler", o.ns)
	if err != nil {
		return err
	}
	err = runCommandWithOSIO("kubectl", "exec", "-it", podNames[0], "-n", o.ns, "-c", "scheduler", "--", "/bin/ls", "-l", "/opt/airflow/dags")
	if err != nil {
		return err
	}
	return nil
}

func (o *dagCmd) clear(parameters []string) error {
	return nil
}

func (o *dagCmd) upload(parameters []string) error {
	listClusters, err := GetNineCLusters(o.ns)
	if err != nil {
		return err
	}
	o.nineName = listClusters.Items[0].Name
	if err != nil {
		return err
	}
	if o.dagsPath != "" {
		err = o.uploadFileToAirflow(o.dagsPath, &listClusters.Items[0])
		if err != nil {
			return err
		}
	}

	return err
}

func (o *dagCmd) run() error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)

	var parameters []string
	if path != "" {
		parameters = append(parameters, []string{"--kubeconfig", path}...)
	}

	switch o.subCommand {
	case "upload":
		err := o.upload(parameters)
		if err != nil {
			return err
		}
	case "clear":
		err := o.clear(parameters)
		if err != nil {
			return err
		}
	case "list":
		err := o.list(parameters)
		if err != nil {
			return err
		}
	}

	return nil
}
