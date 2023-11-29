package cmd

import (
	"fmt"
	"os/exec"
	"strings"
)

const (
	DefaultHelmRepoName = "nineinfra"
	DefaultHelmRepo     = "https://nineinfra.github.io/nineinfra-charts/"
)

func CheckHelmCmdExist() bool {
	_, err := exec.LookPath("helm")
	if err != nil {
		return false
	}
	return true
}

func InstallHelmCmd() error {
	_, _, err := runCommand("curl", "-fsSL", "https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash")
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

func HelmInstall(name string, repoName string, chart string, namespace string, flags string) error {
	if repoName == "" {
		repoName = DefaultHelmRepoName
	}
	chart = repoName + "/" + chart
	if flags == "" {
		_, errput, err := runCommand("helm", "install", name, chart, "-n", namespace)
		if err != nil && !strings.Contains(errput, "in use") {
			return err
		}
	} else {
		_, errput, err := runCommand("helm", "install", name, chart, "-n", namespace, flags)
		if err != nil && !strings.Contains(errput, "in use") {
			return err
		}
	}
	fmt.Printf("Install %s successfully!\n", name)
	return nil
}

func HelmUnInstall(name string, repoName string, namespace string, flags string) error {
	if repoName == "" {
		repoName = DefaultHelmRepoName
	}
	_, errput, err := runCommand("helm", "uninstall", name, "-n", namespace, flags)
	if err != nil && !strings.Contains(errput, "not found") {
		return err
	}
	fmt.Printf("Uninstall %s successfully!\n", name)
	return nil
}
