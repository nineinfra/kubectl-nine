package cmd

const (
	DefaultNamespace         = "nineinfra"
	DefaultMinioPVCLabelKey  = "v1.min.io/tenant"
	DefaultClusterLabelKey   = "cluster"
	DefaultReleaseLabelKey   = "release"
	DefaultAppLabelKey       = "app"
	DefaultNineSuffix        = "-nine"
	DefaultThriftPortName    = "thrift-binary"
	DefaultCMDHelm           = "helm"
	DefaultCMDDirectPV       = "kubectl-directpv"
	DefaultNineInfraPrefix   = "nineinfra"
	GiMultiplier             = 1024 * 1024 * 1024
	DefaultSqlAccessUserName = "kyuubi"
	DefaultSqlAccessPassword = "kyuubi"
)
const (
	DefaultPGRWSVCNameSuffix             = DefaultNineSuffix + "-pg-rw"
	DefaultPGRWPortName                  = "postgres"
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
	DefaultAirflowTierPVCLabelKey        = "tier"
	DefaultToolNifiUserName              = "admin"
	DefaultToolNifiUserPWD               = "nineinfraadmin"
	DefaultZookeeperHLSVCNameSuffix      = "zookeeper-headless"
	DefaultZookeeperPVCLabelKey          = "app.kubernetes.io/instance"
)

const (
	DefaultTPCDSPrefix       = DefaultNineInfraPrefix + "-tpcds-spark"
	DefaultTPCDSAPP          = DefaultNineInfraPrefix + "-tpcds-spark"
	ValidSparkDeployModeList = "client,cluster"
	SparkDeployModeCluster   = "cluster"
	SparkDriverNameSuffix    = "-driver"
	DefaultSparkUINodePort   = 31334
	DefaultSparkUIPort       = 4040
	DefaultSparkUIName       = "spark-ui"
)

const (
	FeaturesOlapKey           = "olap"
	FeaturesKyuubiHAKey       = "kyuubi-ha"
	FeaturesStorageKey        = "storage"
	FeaturesStorageValueHdfs  = "hdfs"
	FeaturesStorageValueMinio = "minio"
	FeaturesOlapValueDoris    = "doris"
)

const (
	DefaultDorisPortName     = "query-port"
	DefaultOlapPVCLabelKey   = "app.doris.ownerreference/name"
	DefaultDorisBENameSuffix = "-doris-be"
	DefaultDorisFENameSuffix = "-doris-fe"
)

var (
	DefaultZookeeperReplicas = 3
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
	DefaultStorageClass          = "nineinfra-default"
	DefaultToolNifiSideCarTag    = "1.36.1"
	DefaultAccessHost            = ""
	DefaultDorisAdminUser        = "root"
	DefaultDorisAdminPassword    = ""
	DefaultDorisDatabaseName     = "nineinfra"
	DefaultDorisFERepo           = "selectdb/doris.fe-ubuntu"
	DefaultDorisFEVersion        = "2.0.2"
	DefaultDorisFERepoPullPolicy = "IfNotPresent"
	DefaultDorisFEStoragePVSize  = 20
	DefaultDorisBERepo           = "selectdb/doris.be-ubuntu"
	DefaultDorisBEVersion        = "2.0.2"
	DefaultDorisBERepoPullPolicy = "IfNotPresent"
	DefaultDorisBEStoragePVSize  = 100
	DefaultKyuubiUserName        = "hive"
	DefaultKyuubiVersion         = "1.8.0"
	DefaultScalaVersion          = "2.12"
	DefaultMinioRepo             = "minio/minio"
	DefaultMinioVersion          = "RELEASE.2023-09-07T02-05-02Z"
	DefaultMinioRepoPullPolicy   = "IfNotPresent"
	DefaultDataBaseVersion       = "v16.0.0"
)

var (
	OlapsSupported       = "doris"
	MainStorageSupported = []string{FeaturesStorageValueHdfs, FeaturesStorageValueMinio}
)

var NineInfraCrdList = []string{
	"hdfsclusters.hdfs.nineinfra.tech",
	"kyuubiclusters.kyuubi.nineinfra.tech",
	"metastoreclusters.metastore.nineinfra.tech",
	"nineclusters.nine.nineinfra.tech",
	"zookeeperclusters.zookeeper.nineinfra.tech",
	"backups.postgresql.cnpg.io",
	"clusters.postgresql.cnpg.io",
	"poolers.postgresql.cnpg.io",
	"scheduledbackups.postgresql.cnpg.io",
	"dorisclusters.doris.selectdb.com",
	"directpvdrives.directpv.min.io",
	"directpvvolumes.directpv.min.io",
	"directpvinitrequests.directpv.min.io",
	"directpvnodes.directpv.min.io",
	"tenants.minio.min.io",
}

var DefaultChartList = map[string]string{
	"cloudnative-pg":     "0.19.1",
	"kyuubi-operator":    "0.7.0",
	"metastore-operator": "0.7.0",
	"zookeeper-operator": "0.7.0",
	"hdfs-operator":      "0.7.0",
	"minio-directpv":     "4.0.8",
	"minio-operator":     "5.0.9",
	"doris-operator":     "1.3.1",
	"nineinfra":          "0.7.0",
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
	"doris-operator":                "doris-operator",
	"hdfs-operator-deployment":      "hdfs-operator",
	"zookeeper-operator-deployment": "zookeeper-operator",
}

var NineClusterProjectNameSuffix = map[string]string{
	"kyuubi":      "-nine-kyuubi",
	"metastore":   "-nine-metastore",
	"minio":       "-nine-ss-0",
	"postgresql":  "-nine-pg",
	"doris-fe":    "-nine-doris-fe",
	"doris-be":    "-nine-doris-be",
	"namenode":    "-nine-hdfs-namenode",
	"datanode":    "-nine-hdfs-datanode",
	"journalnode": "-nine-hdfs-journalnode",
	"zookeeper":   "-nine-zookeeper",
}

var NineClusterProjectWorkloadList = map[string]string{
	"kyuubi":     "statefulset",
	"metastore":  "statefulset",
	"postgresql": "cluster",
}

var NineClusterOlapList = map[string]interface{}{
	FeaturesOlapValueDoris: NineClusterOlapDorisWorkloadList,
}

var NineClusterStorageList = map[string]interface{}{
	FeaturesStorageValueMinio: NineClusterStorageMinioWorkloadList,
	FeaturesStorageValueHdfs:  NineClusterStorageHdfsWorkloadList,
}

var NineClusterOlapDorisWorkloadList = map[string]string{
	"doris-fe": "statefulset",
	"doris-be": "statefulset",
}

var NineClusterStorageHdfsWorkloadList = map[string]string{
	"journalnode": "statefulset",
	"datanode":    "statefulset",
	"namenode":    "statefulset",
	"zookeeper":   "statefulset",
}

var NineClusterStorageMinioWorkloadList = map[string]string{
	"minio": "statefulset",
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

type NineInfraStoragePool string

// Enum of NineInfraStoragePool type.
const (
	NineInfraStoragePoolDefault NineInfraStoragePool = "nineinfra-default"
	NineInfraStoragePoolHigh    NineInfraStoragePool = "nineinfra-high"
	NineInfraStoragePoolMedium  NineInfraStoragePool = "nineinfra-medium"
	NineInfraStoragePoolLow     NineInfraStoragePool = "nineinfra-low"
)
