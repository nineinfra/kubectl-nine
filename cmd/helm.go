package cmd

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

const (
	DefaultHelmRepoName = "nineinfra"
	DefaultHelmRepo     = "https://nineinfra.github.io/nineinfra-charts/"
	DefaultHelmCmdURL   = "https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3"
)

func CheckHelmCmdExist() bool {
	_, err := exec.LookPath(DefaultCMDHelm)
	if err != nil {
		return false
	}
	return true
}

func InstallHelmCmd() error {
	_, _, err := runCommand("curl", "-OfsSL", DefaultHelmCmdURL)
	if err != nil {
		return err
	}
	_, _, err = runCommand("bash", "./get-helm-3")
	if err != nil {
		return err
	}
	return nil
}

func AddHelmRepo(repo string) error {
	if repo == "" {
		repo = DefaultHelmRepo
	}
	_, errput, err := runCommand("helm", "repo", "add", DefaultHelmRepoName, repo)
	if err != nil && !strings.Contains(errput, "already exists") {
		return err
	}
	fmt.Printf("Add repo %s successfully\n", repo)
	return nil
}

func RemoveHelmRepo(repo string) error {
	if repo == "" {
		repo = DefaultHelmRepo
	}
	_, errput, err := runCommand("helm", "repo", "remove", DefaultHelmRepoName, repo)
	if err != nil && !strings.Contains(errput, "not found") {
		return err
	}
	fmt.Printf("Remove repo %s successfully\n", repo)
	return nil
}

func InitHelm() error {
	if !CheckHelmCmdExist() {
		err := InstallHelmCmd()
		if err != nil {
			return err
		}
	}
	if err := AddHelmRepo(DefaultHelmRepo); err != nil {
		return err
	}
	return nil
}

func HelmInstall(name string, repoName string, chart string, version string, namespace string, flags string) error {
	if repoName == "" {
		repoName = DefaultHelmRepoName
	}
	chart = repoName + "/" + chart
	if flags == "" {
		_, errput, err := runCommand("helm", "install", name, chart, "--version", version, "-n", namespace)
		if err != nil && !strings.Contains(errput, "in use") {
			return errors.New(errput)
		}
	} else {
		_, errput, err := runCommand("helm", "install", name, chart, "--version", version, "-n", namespace, flags)
		if err != nil && !strings.Contains(errput, "in use") {
			return errors.New(errput)
		}
	}
	fmt.Printf("Install %s successfully!\n", name)
	return nil
}

func HelmInstallWithParameters(name string, repoName string, chart string, version string, namespace string, parameters ...string) error {
	if repoName == "" {
		repoName = DefaultHelmRepoName
	}
	chart = repoName + "/" + chart
	args := []string{"install", name, chart, "--version", version, "-n", namespace}
	args = append(args, parameters...)
	_, errput, err := runCommand("helm", args...)
	if err != nil && !strings.Contains(errput, "in use") {
		return errors.New(errput)
	}

	fmt.Printf("Install %s successfully!\n", name)
	return nil
}

func HelmUnInstall(name string, namespace string, flags string) error {
	_, errput, err := runCommand("helm", "uninstall", name, "-n", namespace, flags)
	if err != nil && !strings.Contains(errput, "not found") {
		return errors.New(errput)
	}
	fmt.Printf("Uninstall %s successfully!\n", name)
	return nil
}

func CheckHelmReleaseExist(name string, namespace string) bool {
	_, errput, err := runCommand("helm", "status", name, "-n", namespace)
	if err != nil {
		if !strings.Contains(errput, "not found") {
			return false
		} else {
			return false
		}
	}
	return true
}
