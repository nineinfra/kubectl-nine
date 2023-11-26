package cmd

const (
	DefaultNamespace   = "nineinfra"
	DefaultPVCLabelKey = "v1.min.io/tenant"
)

var DefaultChartList = []string{
	"cloudnative-pg",
	"kyuubi-operator",
	"metastore-operator",
	"minio-directpv",
	"minio-operator",
	"nineinfra",
}

var NineInfraDeploymentAlias = map[string]string{
	"cloudnative-pg":                "postgresql-operator",
	"kyuubi-operator-deployment":    "kyuubi-operator",
	"metastore-operator-deployment": "metastore-operator",
	"console":                       "minio-console",
	"minio-operator":                "minio-operator",
	"controller":                    "directpv-controller",
	"nineinfra-deployment":          "nineinfra",
}

var NineClusterProjectWorkloadList = map[string]string{
	"-nine-kyuubi":    "statefulset",
	"-nine-metastore": "statefulset",
	"-nine-ss-0":      "stattefulset",
	"-nine-pg":        "cluster",
}
