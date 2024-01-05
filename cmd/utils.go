package cmd

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"net"
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

func CreateIfNotExist(resource string, resourceType string, flags string) error {
	if flags == "" {
		_, errput, err := runCommand("kubectl", "create", resourceType, resource)
		if err != nil && !strings.Contains(errput, "exists") {
			return err
		}
	} else {
		_, errput, err := runCommand("kubectl", "create", resourceType, resource, flags)
		if err != nil && !strings.Contains(errput, "exists") {
			return err
		}
	}

	fmt.Printf("Create %s %s successfully!\n", resourceType, resource)
	return nil
}

//func DeleteIfExist(resource string, resourceType string, flags string) error {
//	_, errput, err := runCommand("kubectl", "delete", resourceType, resource, flags)
//	if err != nil && !strings.Contains(errput, "not found") {
//		return err
//	}
//	fmt.Printf("Delete %s %s successfully!\n", resourceType, resource)
//	return nil
//}

func ValidateClusterArgs(cmd string, args []string) error {
	if args == nil {
		return fmt.Errorf("provide the name of the cluster, e.g. 'kubectl nine %s cluster1 -n c1-ns flags'", cmd)
	}
	if len(args) != 1 {
		return fmt.Errorf("%s command supports a single argument, e.g. 'kubectl nine %s cluster1 -n c1-ns flags'", cmd, cmd)
	}
	if args[0] == "" {
		return fmt.Errorf("provide the name of the cluster, e.g. 'kubectl nine %s cluster1 -n c1-ns flags'", cmd)
	}
	if DefaultAccessHost != "" {
		if net.ParseIP(DefaultAccessHost) == nil {
			return fmt.Errorf("invalid access host %s", DefaultAccessHost)
		}
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
