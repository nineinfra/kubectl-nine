package cmd

const (
	DefaultNamespace      = "nineinfra"
	DefaultPVCLabelKey    = "v1.min.io/tenant"
	DefaultNineSuffix     = "-nine"
	DefaultThriftPortName = "thrift-binary"
	DefaultCMDHelm        = "helm"
	DefaultCMDDirectPV    = "kubectl-directpv"
	DefaultKyuubiUserName = "hive"
)
const (
	DefaultPGRWSVCNameSuffix             = DefaultNineSuffix + "-pg-rw"
	DefaultToolsNamePrefix               = "nineinfra-"
	DefaultRedisSVCName                  = DefaultToolsNamePrefix + "redis"
	DefaultToolAirflowDBUser             = "airflow"
	DefaultToolAirflowDBPwd              = "airflow"
	DefaultToolAirflowDBName             = "airflow"
	DefaultToolSupersetDBUser            = "superset"
	DefaultToolSupersetDBPwd             = "superset"
	DefaultToolSupersetDBName            = "superset"
	DefaultToolAirflowName               = "airflow"
	DefaultToolSupersetName              = "superset"
	DefaultToolNifiName                  = "nifi"
	DefaultToolZookeeperName             = "zookeeper"
	DefaultToolRedisName                 = "redis"
	DefaultToolAirflowWebServerSecretKey = "2ae7138d1fc0859df4a2456dd0146785"
	DefaultToolAirflowDiskSize           = "20Gi"
	DefaultToolNifiUserName              = "admin"
	DefaultToolNifiUserPWD               = "nineinfraadmin"
	DefaultZookeeperSVCName              = DefaultToolsNamePrefix + "zookeeper-headless"
)

var (
	DefaultToolSupersetSecretFile       = "secret"
	DefaultToolSupersetSDataSourcesFile = "import_datasources.yaml"
)

var (
	DEBUG                        = false
	DefaultToolNifiSvcNodePort   = 31333
	DefaultToolSupersetSvcType   = "NodePort"
	DefaultToolNifiSvcType       = "NodePort"
	DefaultToolAirflowSvcType    = "NodePort"
	DefaultToolAirflowRepository = "nineinfra/airflow"
	DefaultToolAirflowTag        = "2.7.3"
	DefaultStorageClass          = "directpv-min-io"
	DefaultToolNifiSideCarTag    = "1.36.1"
)

var DefaultChartList = map[string]string{
	"cloudnative-pg":     "0.19.1",
	"kyuubi-operator":    "0.181.4",
	"metastore-operator": "0.313.3",
	"minio-directpv":     "4.0.8",
	"minio-operator":     "5.0.9",
	"nineinfra":          "0.4.4",
}

var DefaultToolsChartList = map[string]string{
	"airflow":   "1.12.0",
	"superset":  "0.11.2",
	"nifi":      "1.1.6",
	"zookeeper": "12.3.3",
	"redis":     "0.7.5",
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

var NineToolList = map[string]interface{}{
	DefaultToolAirflowName:   NineToolAirflowWorkloadList,
	DefaultToolSupersetName:  NineToolSupersetloadList,
	DefaultToolNifiName:      NineToolNifiWorkloadList,
	DefaultToolRedisName:     NineToolRedisWorkloadList,
	DefaultToolZookeeperName: NineToolZookeeperWorkloadList,
}

var NineToolAirflowWorkloadList = map[string]string{
	"airflow-webserver": "deployment",
	"airflow-scheduler": "deployment",
	"airflow-triggerer": "statefulset",
	"airflow-worker":    "statefulset",
}

var NineToolSupersetloadList = map[string]string{
	"superset":          "deployment",
	"superset-worker":   "deployment",
	"airflow-triggerer": "statefulset",
	"airflow-worker":    "statefulset",
}

var NineToolNifiWorkloadList = map[string]string{
	"nifi": "statefulset",
}

var NineToolRedisWorkloadList = map[string]string{
	"redis": "deployment",
}

var NineToolZookeeperWorkloadList = map[string]string{
	"zookeeper": "statefulset",
}

var NineToolSvcList = map[string]string{
	DefaultToolAirflowName:   "airflow-webserver",
	DefaultToolSupersetName:  "superset",
	DefaultToolNifiName:      "nifi",
	DefaultToolRedisName:     "redis",
	DefaultToolZookeeperName: "zookeeper",
}

var NineToolPortNameList = map[string]string{
	DefaultToolAirflowName:   "airflow-ui",
	DefaultToolSupersetName:  "http",
	DefaultToolNifiName:      "https",
	DefaultToolRedisName:     "redis",
	DefaultToolZookeeperName: "tcp-client",
}

var NineToolPortProtocolList = map[string]string{
	DefaultToolAirflowName:   "http",
	DefaultToolSupersetName:  "http",
	DefaultToolNifiName:      "https",
	DefaultToolRedisName:     "redis",
	DefaultToolZookeeperName: "",
}
