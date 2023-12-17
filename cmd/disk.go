package cmd

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"k8s.io/klog/v2"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	diskDesc    = `'disk' command manages the physical disks on the k8s for the NineCluster`
	diskExample = `1. Discover drives
   $ kubectl nine disk discover

2. Discover drives from a node
   $ kubectl nine disk discover --nodes=node1

3. Discover a drive from all nodes
   $ kubectl nine disk discover --drives=nvme1n1

4. Discover all drives from all nodes (including unavailable)
   $ kubectl nine disk discover --all

5. Discover specific drives from specific nodes
   $ kubectl nine disk discover --nodes=node{1...4} --drives=sd{a...f}

6. Initialize the drives
   $ kubectl nine disk init drives.yaml

7. Remove an unused drive from all nodes
   $ kubectl nine disk remove --drives=nvme1n1

8. Remove all unused drives from a node
   $ kubectl nine disk remove --nodes=node1

9. Remove specific unused drives from specific nodes
   $ kubectl nine disk remove --nodes=node{1...4} --drives=sd{a...f}

10. Remove all unused drives from all nodes
   $ kubectl nine disk remove --all

11. Remove drives are in 'error' status
   $ kubectl nine disk remove --status=error

12. List drives
   $ kubectl nine disk list drives

13. List volumes
   $ kubectl nine disk list volumes`
)

// DriveStatus denotes drive status
type DriveStatus string

const (
	// DriveStatusReady denotes drive is ready for volume schedule.
	DriveStatusReady DriveStatus = "Ready"

	// DriveStatusLost denotes associated data by FSUUID is lost.
	DriveStatusLost DriveStatus = "Lost"

	// DriveStatusError denotes drive is in error state to prevent volume schedule.
	DriveStatusError DriveStatus = "Error"

	// DriveStatusRemoved denotes drive is removed.
	DriveStatusRemoved DriveStatus = "Removed"

	// DriveStatusMoving denotes drive is moving volumes.
	DriveStatusMoving DriveStatus = "Moving"
)

var driveStatusValues = []string{
	strings.ToLower(string(DriveStatusError)),
	strings.ToLower(string(DriveStatusLost)),
	strings.ToLower(string(DriveStatusMoving)),
	strings.ToLower(string(DriveStatusReady)),
	strings.ToLower(string(DriveStatusRemoved)),
}

var (
	outputFile      = "drives.yaml"
	nodeListTimeout = 2 * time.Minute
	subCommandList  = "discover,init,remove,list"
)

type diskCmd struct {
	out             io.Writer
	errOut          io.Writer
	subCommand      string
	subArg          string
	outputFormat    string   // --output flag
	nodesArgs       []string // --nodes flag
	drivesArgs      []string // --drives flag
	driveStatusArgs []string // --status flag of drives
	allFlag         bool     // --all flag
	dangerousFlag   bool     // --dangerous flag
	dryRunFlag      bool     // --dry-run flag
	noHeaders       bool     // --no-headers flag
}

func newDiskCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &diskCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "disk <SUBCOMMAND>",
		Short:   "Manage the physical disks on the k8s for the NineCluster",
		Long:    diskDesc,
		Example: diskExample,
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
	f.StringSliceVarP(&c.nodesArgs, "nodes", "n", c.nodesArgs, "discover drives from given nodes; supports ellipses pattern e.g. node{1...10}")
	f.StringSliceVarP(&c.drivesArgs, "drives", "d", c.drivesArgs, "discover drives by given names; supports ellipses pattern e.g. sd{a...z}")
	f.BoolVar(&c.allFlag, "all", c.allFlag, "If present, include non-formattable devices in the display")
	f.StringVar(&outputFile, "output-file", outputFile, "output file to write the init config")
	f.DurationVar(&nodeListTimeout, "timeout", nodeListTimeout, "specify timeout for the discovery process")
	f.BoolVar(&c.dangerousFlag, "dangerous", c.dangerousFlag, "Perform initialization of drives which will permanently erase existing data")
	f.StringSliceVar(&c.driveStatusArgs, "status", c.driveStatusArgs, fmt.Sprintf("%v; one of: %v", "If present, select drives by drive status", strings.Join(driveStatusValues, "|")))
	f.BoolVar(&c.dryRunFlag, "dry-run", c.dryRunFlag, "Run in dry run mode")
	f.StringVarP(&c.outputFormat, "output", "o", c.outputFormat, "Output format. One of: json|yaml|wide")
	f.BoolVar(&c.noHeaders, "no-headers", c.noHeaders, "When using the default or custom-column output format, don't print headers (default print headers)")
	return cmd
}

func (d *diskCmd) validate(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("not enough parameters")
	}
	d.subCommand = args[0]
	if !strings.Contains(subCommandList, d.subCommand) {
		return fmt.Errorf("unsupported subcommand %s, only %s supported", d.subCommand, subCommandList)
	}
	switch d.subCommand {
	case "init":
		if len(args) != 2 {
			return fmt.Errorf("please provide the input file")
		}
		d.subArg = args[1]
	case "discover":
		if len(args) != 1 {
			return fmt.Errorf("too many input args")
		}
	case "remove":
		if len(args) != 1 {
			return fmt.Errorf("too many input args")
		}
	case "list":
		if len(args) != 2 {
			return fmt.Errorf("please provide arg for list,support[drives,volumes]")
		}
		d.subArg = args[1]
	}
	return nil
}

func (d *diskCmd) run(_ []string) error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	var parameters []string
	if d.subArg != "" {
		parameters = []string{"directpv", d.subCommand, d.subArg}
	} else {
		parameters = []string{"directpv", d.subCommand}
	}
	if path != "" {
		parameters = append(parameters, []string{"--kubeconfig", path}...)
	}

	if len(d.nodesArgs) != 0 {
		parameters = append(parameters, []string{"--nodes", strings.Join(d.nodesArgs, ",")}...)
	}

	if len(d.drivesArgs) != 0 {
		parameters = append(parameters, []string{"--drives", strings.Join(d.drivesArgs, ",")}...)
	}
	if len(d.driveStatusArgs) != 0 {
		parameters = append(parameters, []string{"--status", strings.Join(d.driveStatusArgs, ",")}...)
	}
	if d.allFlag {
		parameters = append(parameters, []string{"--all"}...)
	}
	if d.dangerousFlag {
		parameters = append(parameters, []string{"--dangerous"}...)
	}
	if d.dryRunFlag {
		parameters = append(parameters, []string{"--dry-run"}...)
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
		os.Exit(1)
	}
	err = cmd.Wait()
	if err != nil {
		os.Exit(1)
	}

	return nil
}
