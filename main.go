package main

import (
	"github.com/nineinfra/kubectl-nine/cmd"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"os"
)

func main() {
	if err := cmd.New(genericiooptions.IOStreams{
		In:     os.Stdin,
		Out:    os.Stdout,
		ErrOut: os.Stderr,
	}).Execute(); err != nil {
		os.Exit(1)
	}
}
