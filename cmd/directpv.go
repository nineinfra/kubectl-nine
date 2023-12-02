package cmd

import (
	"os/exec"
)

const (
	CMDDirectPVURL         = "https://github.com/minio/directpv/releases/download/v4.0.9/kubectl-directpv_4.0.9_linux_amd64"
	CMDDirectPVInstallPath = "/usr/local/bin/"
)

func CheckDirectPVCmdExist() bool {
	_, err := exec.LookPath(CMDDirectPV)
	if err != nil {
		return false
	}
	return true
}

func InstallDirectPVCmd() error {
	_, _, err := runCommand("curl", "-o", CMDDirectPVInstallPath+CMDDirectPV, "-fsSL", CMDDirectPVURL)
	if err != nil {
		return err
	}
	_, _, err = runCommand("chmod", "0755", CMDDirectPVInstallPath+CMDDirectPV)
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
