package cmd

import (
	"bufio"
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os/exec"
	"strings"
	"time"
)

const (
	storageDesc    = `'storage' command manages the physical storages on the k8s for the NineCluster`
	storageExample = `1. Create storage pool
   $ kubectl nine storage --command=create --nodes=node{1...4} --drives=sd{a...f} --storage-pool=nineinfra-high --dangerous

2. Delete storage pool
   $ kubectl nine storage -c=delete --storage-pool=nineinfra-high

3. List storage pools
   $ kubectl nine storage -c=list`
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

const (
	DiskSubCommandDiscover = "discover"
	DiskSubCommandInit     = "init"
	DiskSubCommandLabel    = "label"
)

var driveStatusValues = []string{
	strings.ToLower(string(DriveStatusError)),
	strings.ToLower(string(DriveStatusLost)),
	strings.ToLower(string(DriveStatusMoving)),
	strings.ToLower(string(DriveStatusReady)),
	strings.ToLower(string(DriveStatusRemoved)),
}

var (
	outputFile            = "drives.yaml"
	nodeListTimeout       = 2 * time.Minute
	subCommandList        = "create,delete,list,clean,remove"
	storagePoolsSupported = "nineinfra-default,nineinfra-high,nineinfra-medium,nineinfra-low"
)

type storageCmd struct {
	out             io.Writer
	errOut          io.Writer
	ns              string
	nineName        string
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
	storagePool     string
}

func newStorageCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &storageCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "storage",
		Short:   "Manage the physical storages on the k8s for the NineCluster",
		Long:    storageDesc,
		Example: storageExample,
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
	f.StringSliceVar(&c.nodesArgs, "nodes", c.nodesArgs, "discover drives from given nodes; supports ellipses pattern e.g. node{1...10}")
	f.StringSliceVar(&c.drivesArgs, "drives", c.drivesArgs, "discover drives by given names; supports ellipses pattern e.g. sd{a...z}")
	f.StringVarP(&c.subCommand, "command", "c", "", fmt.Sprintf("command for storage,[%s] are supported now", subCommandList))
	f.BoolVar(&c.allFlag, "all", c.allFlag, "If present, include non-formattable devices in the display")
	f.StringVar(&outputFile, "output-file", outputFile, "output file to write the init config")
	f.StringVar(&c.storagePool, "storage-pool", "nineinfra-default", fmt.Sprintf("specify the storage pool name,support [%s]", storagePoolsSupported))
	f.DurationVar(&nodeListTimeout, "timeout", nodeListTimeout, "specify timeout for the discovery process")
	f.BoolVar(&c.dangerousFlag, "dangerous", c.dangerousFlag, "Perform initialization of drives which will permanently erase existing data")
	f.StringSliceVar(&c.driveStatusArgs, "status", c.driveStatusArgs, fmt.Sprintf("%v; one of: %v", "If present, select drives by drive status", strings.Join(driveStatusValues, "|")))
	f.BoolVar(&c.dryRunFlag, "dry-run", c.dryRunFlag, "Run in dry run mode")
	f.StringVarP(&c.outputFormat, "output", "o", c.outputFormat, "Output format. One of: json|yaml|wide")
	f.StringVarP(&c.ns, "namespace", "n", "", "k8s namespace for storage pvcs")
	f.StringVar(&c.nineName, "ninecluster-name", "", "the name of the ninecluster")
	f.BoolVar(&c.noHeaders, "no-headers", c.noHeaders, "When using the default or custom-column output format, don't print headers (default print headers)")
	return cmd
}

func (d *storageCmd) validate(args []string) error {
	if !strings.Contains(subCommandList, d.subCommand) {
		return fmt.Errorf("unsupported subcommand %s, only %s supported", d.subCommand, subCommandList)
	}
	switch d.subCommand {
	case "create":
		if !strings.Contains(storagePoolsSupported, d.storagePool) {
			return fmt.Errorf("please provide valid storage pool name:%s,support[%s]", d.storagePool, storagePoolsSupported)
		}
	case "delete":
		if !strings.Contains(storagePoolsSupported, d.storagePool) {
			return fmt.Errorf("please provide valid storage pool name:%s,support[%s]", d.storagePool, storagePoolsSupported)
		}
	case "clean":
		if !strings.Contains(storagePoolsSupported, d.storagePool) {
			return fmt.Errorf("please provide valid storage pool name:%s,support[%s]", d.storagePool, storagePoolsSupported)
		}
	}
	return nil
}

func (d *storageCmd) addFlags(parameters []string, subCommand string) []string {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	if path != "" {
		parameters = append(parameters, []string{"--kubeconfig", path}...)
	}
	if subCommand == DiskSubCommandDiscover || subCommand == DiskSubCommandLabel {
		if len(d.nodesArgs) != 0 {
			parameters = append(parameters, []string{"--nodes", strings.Join(d.nodesArgs, ",")}...)
		}

		if len(d.drivesArgs) != 0 {
			parameters = append(parameters, []string{"--drives", strings.Join(d.drivesArgs, ",")}...)
		}
	}
	if len(d.driveStatusArgs) != 0 {
		parameters = append(parameters, []string{"--status", strings.Join(d.driveStatusArgs, ",")}...)
	}
	if d.allFlag {
		parameters = append(parameters, []string{"--all"}...)
	}
	if subCommand == DiskSubCommandInit {
		if d.dangerousFlag {
			parameters = append(parameters, []string{"--dangerous"}...)
		}
	}
	if d.dryRunFlag {
		parameters = append(parameters, []string{"--dry-run"}...)
	}
	return parameters
}

func (d *storageCmd) executeKubectlCommand(parameters []string) error {
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
		return err
	}
	err = cmd.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (d *storageCmd) executeDiskCmd(parameters []string, subCommand string) error {
	parameters = d.addFlags(parameters, subCommand)
	return d.executeKubectlCommand(parameters)
}

func (d *storageCmd) createStorageClass() error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetKubeClient(path)
	if err != nil {
		return err
	}

	_, err = client.StorageV1().StorageClasses().Get(context.TODO(), d.storagePool, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	allowVolumeExpansion := true
	reclaimPolicy := corev1.PersistentVolumeReclaimDelete
	volumeBindingMode := storagev1.VolumeBindingWaitForFirstConsumer
	if k8serrors.IsNotFound(err) {
		desiredSC := &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: d.storagePool,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "NineInfra",
					"application-name":             "directpv.min.io",
					"application-type":             "CSIDriver",
					"directpv.min.io/created-by":   "kubectl-nine",
					"directpv.min.io/version":      "v1beta1",
				},
			},
			AllowVolumeExpansion: &allowVolumeExpansion,
			AllowedTopologies: []corev1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{
						{
							Key:    "directpv.min.io/identity",
							Values: []string{"directpv-min-io"},
						},
					},
				},
			},
			Parameters: map[string]string{
				"fstype":                       "xfs",
				"directpv.min.io/storage-pool": d.storagePool,
			},
			Provisioner:       "directpv-min-io",
			ReclaimPolicy:     &reclaimPolicy,
			VolumeBindingMode: &volumeBindingMode,
		}
		_, err = client.StorageV1().StorageClasses().Create(context.TODO(), desiredSC, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}

	var parameters []string
	parameters = []string{"directpv", DiskSubCommandLabel, "drives", fmt.Sprintf("storage-pool=%s", d.storagePool)}
	err = d.executeDiskCmd(parameters, DiskSubCommandLabel)
	if err != nil {
		return err
	}
	return nil
}

func (d *storageCmd) runCreateCmd() error {
	var parameters []string

	parameters = []string{"directpv", DiskSubCommandDiscover}

	err := d.executeDiskCmd(parameters, DiskSubCommandDiscover)
	if err != nil {
		return err
	}

	parameters = []string{"directpv", DiskSubCommandInit, outputFile}
	err = d.executeDiskCmd(parameters, DiskSubCommandInit)
	if err != nil {
		return err
	}

	err = d.createStorageClass()
	if err != nil {
		return err
	}

	return nil
}

func (d *storageCmd) runCleanCmd() error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetKubeClient(path)
	if err != nil {
		return err
	}
	pvList, err := GetReleasedAndDeletePolicyPVListByStorageClass(client, d.storagePool)
	if err != nil {
		return err
	}
	for _, pv := range pvList.Items {
		err = client.CoreV1().PersistentVolumes().Delete(context.TODO(), pv.Name, metav1.DeleteOptions{})
		if err != nil {
			return err
		}
		fmt.Printf("Delete the Released and Deleted pv %s in the storagepool %s successfully!\n", pv.Name, d.storagePool)
	}
	if d.ns != "" && d.nineName != "" {
		directpvClient, err := GetDirectPVClient(path)
		if err != nil {
			return err
		}
		directpvList, err := GetReadyDirectPVVolumes(directpvClient, d.ns, NineResourceName(d.nineName))
		if err != nil {
			return err
		}
		if directpvList != nil {
			for _, directpv := range directpvList.Items {
				directpv.SetFinalizers(nil)
				_, err = directpvClient.DirectPVVolumes().Update(context.TODO(), &directpv, metav1.UpdateOptions{})
				if err != nil {
					return err
				}
				err = directpvClient.DirectPVVolumes().Delete(context.TODO(), directpv.Name, metav1.DeleteOptions{})
				if err != nil && !k8serrors.IsNotFound(err) {
					return err
				}
				fmt.Printf("Delete the directpv %s of the ninecluster:%s in namespace:%s successfully!\n", directpv.Name, d.nineName, d.ns)
			}
		}
	}
	return nil
}

func (d *storageCmd) runDeleteCmd() error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetKubeClient(path)
	if err != nil {
		return err
	}
	err = client.StorageV1().StorageClasses().Delete(context.TODO(), d.storagePool, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (d *storageCmd) runListCmd() error {
	var parameters []string

	parameters = []string{"get", "storageclass"}
	err := d.executeKubectlCommand(parameters)
	if err != nil {
		return err
	}
	return nil
}

func (d *storageCmd) run(_ []string) error {
	switch d.subCommand {
	case "create":
		err := d.runCreateCmd()
		if err != nil {
			return err
		}
	case "delete":
		err := d.runDeleteCmd()
		if err != nil {
			return err
		}
	case "clean":
		err := d.runCleanCmd()
		if err != nil {
			return err
		}
	case "remove":
		err := d.runCleanCmd()
		if err != nil {
			return err
		}
	case "list":
		err := d.runListCmd()
		if err != nil {
			return err
		}
	default:
		var parameters []string
		if d.subArg != "" {
			parameters = []string{"directpv", d.subCommand, d.subArg}
		} else {
			parameters = []string{"directpv", d.subCommand}
		}

		parameters = d.addFlags(parameters, d.subCommand)

		err := d.executeKubectlCommand(parameters)
		if err != nil {
			return err
		}
	}

	return nil
}
