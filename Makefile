PWD := $(shell pwd)

LDFLAGS ?= "-s -w"

plugin:
	CGO_ENABLED=0 go build -trimpath --ldflags $(LDFLAGS) -o kubectl-nine .

