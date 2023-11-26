package cmd

import (
	"k8s.io/apimachinery/pkg/api/errors"
	"os"
	"os/exec"
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
	cmd := exec.Command("curl", "-fsSL", "https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

func AddHelmRepo(repo string) error {
	if repo == "" {
		repo = DefaultHelmRepo
	}
	cmd := exec.Command("helm", "repo", "add", DefaultHelmRepoName, repo)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func RemoveHelmRepo(repo string) error {
	if repo == "" {
		repo = DefaultHelmRepo
	}
	cmd := exec.Command("helm", "repo", "remove", DefaultHelmRepoName, repo)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
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
	cmd := exec.Command("helm", "install", name, chart, "-n", namespace, flags)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func HelmUnInstall(name string, repoName string, namespace string, flags string) error {
	if repoName == "" {
		repoName = DefaultHelmRepoName
	}
	cmd := exec.Command("helm", "uninstall", name, "-n", namespace, flags)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}
