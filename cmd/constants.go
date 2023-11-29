package cmd

const (
	DefaultNamespace   = "nineinfra"
	DefaultPVCLabelKey = "v1.min.io/tenant"
	DefaultNineSuffix  = "-nine"
)

var DEBUG = false

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

var NineClusterProjectNameSuffix = map[string]string{
	"kyuubi":     "-nine-kyuubi",
	"metastore":  "-nine-metastore",
	"minio":      "-nine-ss-0",
	"postgresql": "-nine-pg",
}

var NineClusterProjectWorkloadList = map[string]string{
	"kyuubi":     "statefulset",
	"metastore":  "statefulset",
	"minio":      "statefulset",
	"postgresql": "cluster",
}
