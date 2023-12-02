package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"k8s.io/klog/v2"
	"os"
)

const (
	prepareDesc    = `'prepare' command check and prepare tools for the nine`
	prepareExample = ` kubectl nine prepare`
)

type prepareCmd struct {
	out    io.Writer
	errOut io.Writer
}

func newPrepareCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &prepareCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "prepare",
		Short:   "Check and prepare tools for the nine",
		Long:    prepareDesc,
		Example: prepareExample,
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
	f.BoolVar(&DEBUG, "debug", false, "print debug infomation")
	return cmd
}

func (o *prepareCmd) run(_ []string) error {
	if err := InitHelm(); err != nil {
		os.Exit(1)
	}
	if err := InitDirectPV(); err != nil {
		os.Exit(1)
	}
	fmt.Println("The Nine is OK!")
	return nil
}
