package cmd

import (
	"os/exec"
)

const (
	DefaultCMDDirectPVURL         = "https://github.com/minio/directpv/releases/download/v4.0.9/kubectl-directpv_4.0.9_linux_amd64"
	DefaultCMDDirectPVInstallPath = "/usr/local/bin/"
)

func CheckDirectPVCmdExist() bool {
	_, err := exec.LookPath(DefaultCMDDirectPV)
	if err != nil {
		return false
	}
	return true
}

func InstallDirectPVCmd() error {
	_, _, err := runCommand("curl", "-o", DefaultCMDDirectPVInstallPath+DefaultCMDDirectPV, "-fsSL", DefaultCMDDirectPVURL)
	if err != nil {
		return err
	}
	_, _, err = runCommand("chmod", "0755", DefaultCMDDirectPVInstallPath+DefaultCMDDirectPV)
	if err != nil {
		return err
	}
	return nil
}

func InitDirectPV() error {
	if !CheckDirectPVCmdExist() {
		err := InstallDirectPVCmd()
		if err != nil {
			return err
		}
	}
	return nil
}
