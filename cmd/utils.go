package cmd

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

var (
	validClusterName = regexp.MustCompile(`^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$`)
	ipAddress        = regexp.MustCompile(`^(\d+\.){3}\d+$`)
)

// DisableHelp disables the help command
func DisableHelp(cmd *cobra.Command) *cobra.Command {
	cmd.SetHelpCommand(&cobra.Command{
		Use:    "no-help",
		Hidden: true,
	})
	return cmd
}

func CreateIfNotExist(resource string, flags string) error {
	cmd := exec.Command("kubectl", "create", resource, flags)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func DeleteIfExist(resource string, flags string) error {
	cmd := exec.Command("kubectl", "delete", resource, flags)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	return nil
}

func ValidateClusterArgs(cmd string, args []string) error {
	if args == nil {
		return fmt.Errorf("provide the name of the cluster, e.g. 'kubectl nine %s cluster1 -n c1-ns'", cmd)
	}
	if len(args) != 1 {
		return fmt.Errorf("%s command supports a single argument, e.g. 'kubectl nine %s cluster1 -n c1-ns'", cmd, cmd)
	}
	if args[0] == "" {
		return fmt.Errorf("provide the name of the cluster, e.g. 'kubectl nine %s cluster1 -n c1-ns'", cmd)
	}
	return CheckValidClusterName(args[0])
}

// CheckValidClusterName validates if input clustername complies with expected restrictions.
func CheckValidClusterName(clustername string) error {
	if strings.TrimSpace(clustername) == "" {
		return errors.New("Cluster name cannot be empty")
	}
	if len(clustername) > 63 {
		return errors.New("Cluster name cannot be longer than 63 characters")
	}
	if ipAddress.MatchString(clustername) {
		return errors.New("Cluster name cannot be an ip address")
	}
	if strings.Contains(clustername, "..") || strings.Contains(clustername, ".-") || strings.Contains(clustername, "-.") {
		return errors.New("Cluster name contains invalid characters")
	}
	if !validClusterName.MatchString(clustername) {
		return errors.New("Cluster name contains invalid characters")
	}
	return nil
}
