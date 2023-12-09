package cmd

const (
	DefaultNamespace      = "nineinfra"
	DefaultPVCLabelKey    = "v1.min.io/tenant"
	DefaultNineSuffix     = "-nine"
	DefaultThriftPortName = "thrift-binary"
	CMDHelm               = "helm"
	CMDDirectPV           = "kubectl-directpv"
)

var DEBUG = false

var DefaultChartList = map[string]string{
	"cloudnative-pg":     "0.19.1",
	"kyuubi-operator":    "0.181.4",
	"metastore-operator": "0.313.3",
	"minio-directpv":     "4.0.8",
	"minio-operator":     "5.0.9",
	"nineinfra":          "0.4.4",
}

var DefaultToolsChartList = map[string]string{
	"airflow":  "1.12.0",
	"superset": "0.11.2",
	"nifi":     "1.1.6",
}

var DefaultBaseComponentsChartList = map[string]string{
	"zookeeper": "12.3.3",
	"redis":     "18.4.0",
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
