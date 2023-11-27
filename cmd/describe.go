package cmd

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"k8s.io/klog/v2"
	"os"
	"os/exec"
)

const (
	describeDesc    = `'describe' command displays the NineCluster's detail information`
	describeExample = ` kubectl nine describe c1 --namespace c1-ns`
)

type describeCmd struct {
	out    io.Writer
	errOut io.Writer
	name   string
	ns     string
}

func newClusterDescribeCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &describeCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "describe <NINECLUSTERNAME> --namespace <NINECLUSTERNS>",
		Short:   "Display the NineCluster's detail information",
		Long:    describeDesc,
		Example: describeExample,
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

func (d *describeCmd) validate(args []string) error {
	d.name = args[0]
	return ValidateClusterArgs("describe", args)
}

func (d *describeCmd) run(_ []string) error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)

	parameters := []string{"describe", "ninecluster", d.name, "-n", d.ns}
	if path != "" {
		parameters = append([]string{"--kubeconfig", path}, parameters...)
	}

	cmd := exec.Command("kubectl", parameters...)

	stdoutReader, _ := cmd.StdoutPipe()
	stdoutScanner := bufio.NewScanner(stdoutReader)
	go func() {
		for stdoutScanner.Scan() {
			fmt.Println(stdoutScanner.Text())
		}
	}()
	stderrReader, _ := cmd.StderrPipe()
	stderrScanner := bufio.NewScanner(stderrReader)
	go func() {
		for stderrScanner.Scan() {
			fmt.Println(stderrScanner.Text())
		}
	}()
	err := cmd.Start()
	if err != nil {
		fmt.Printf("Error : %v \n", err)
		os.Exit(1)
	}
	err = cmd.Wait()
	if err != nil {
		fmt.Printf("Error: %v \n", err)
		os.Exit(1)
	}

	return nil
}
