package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
	"io"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net"
	"os"
	"strings"
)

const (
	toolsDesc    = `'tools' command manages the lifecycle of the toolkit for the NineCluster`
	toolsExample = `1. Install tools for the NineCluster
   $ kubectl nine tools --command=install --namespace=ns

2. Uninstall tools from a namespace
   $ kubectl nine tools --command=uninstall --namespace=ns

3. Install some of the tools for a NineCluster
   $ kubectl nine tools --command=install --toolkit=superset,airflow,nifi --namespace=ns

4. Uninstall some of the tools from a namespace
   $ kubectl nine tools --command=uninstall --toolkit=superset,airflow --namespace=ns

5. List tools
   $ kubectl nine tools -c=list -n=ns`
)

var (
	toolsSubCommandList = "install,uninstall,list"
	toolsSupported      = "superset,airflow,nifi,redis,zookeeper"
)

type toolsCmd struct {
	out         io.Writer
	errOut      io.Writer
	subCommand  string
	ns          string
	nineName    string
	toolkitArgs []string // --nodes flag
	deletePVC   bool
	chartPath   string
	storagepool string
	zkSvcName   string
}

type DatabasesConnection struct {
	AllowFileUpload bool     `yaml:"allow_file_upload"`
	AllowCTAS       bool     `yaml:"allow_ctas"`
	AllowCVAS       bool     `yaml:"allow_cvas"`
	DatabaseName    string   `yaml:"database_name"`
	Extra           string   `yaml:"extra"`
	SqlAlchemyURI   string   `yaml:"sqlalchemy_uri"`
	Tables          []string `yaml:"tables"`
}

func newToolsCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &toolsCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "tools",
		Short:   "Manage the lifecycle of the tools for the NineCluster",
		Long:    toolsDesc,
		Example: toolsExample,
		Args: func(cmd *cobra.Command, args []string) error {
			return c.validate(args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := c.run()
			if err != nil {
				return err
			}
			return nil
		},
	}
	cmd = DisableHelp(cmd)
	f := cmd.Flags()
	f.StringSliceVarP(&c.toolkitArgs, "toolkit", "t", strings.Split(toolsSupported, ","), "toolkit list for the NineCluster")
	f.StringVar(&DefaultAccessHost, "access-host", "", "access host ip for out cluster access,such as web access")
	f.StringVarP(&c.subCommand, "command", "c", "", fmt.Sprintf("command for tools,%s are supported now", toolsSubCommandList))
	f.IntVar(&DefaultToolNifiSvcNodePort, "nifi-nodeport", 31333, "nodePort value for nifi https")
	f.StringVar(&DefaultToolAirflowSvcType, "airflow-svctype", "NodePort", "service type for airflow ui")
	f.StringVar(&DefaultToolSupersetSvcType, "superset-svctype", "NodePort", "service type for superset ui")
	f.StringVar(&DefaultToolNifiSvcType, "nifi-svctype", "NodePort", "service type for nifi ui")
	f.StringVar(&DefaultToolAirflowRepository, "airflow-repo", "nineinfra/airflow", "airflow image repository")
	f.StringVar(&DefaultToolAirflowTag, "airflow-tag", "2.7.3", "airflow image tag")
	f.StringVarP(&c.storagepool, "storage-pool", "s", DefaultStorageClass, "storage pool for tools")
	f.BoolVar(&c.deletePVC, "delete-pvc", false, "delete the ninecluster tools pvcs")
	f.StringVarP(&c.chartPath, "chart-path", "p", "", "local path of the charts")
	f.StringVarP(&c.ns, "namespace", "n", "", "k8s namespace for tools")
	f.BoolVar(&DEBUG, "debug", false, "print debug information")
	return cmd
}

func (t *toolsCmd) validate(args []string) error {
	//if len(args) < 1 {
	//	return fmt.Errorf("not enough parameters")
	//}
	//t.subCommand = args[0]
	if !strings.Contains(toolsSubCommandList, t.subCommand) {
		return fmt.Errorf("unsupported subcommand %s, only %s supported", t.subCommand, toolsSubCommandList)
	}
	for _, v := range t.toolkitArgs {
		if !strings.Contains(toolsSupported, v) {
			return fmt.Errorf("unsupported toolkit %s, only %s supported", v, toolsSupported)
		}
	}
	if DefaultAccessHost != "" {
		if net.ParseIP(DefaultAccessHost) == nil {
			return fmt.Errorf("invalid access host %s", DefaultAccessHost)
		}
	}
	if t.storagepool != DefaultStorageClass {
		if !CheckStoragePoolValid(t.storagepool) {
			return errors.New(fmt.Sprintf("tools storage pool %s may be not exist", t.storagepool))
		}
	}
	return nil
}

func (t *toolsCmd) deleteToolsPVC(namespace string) error {
	if namespace == "" {
		return errors.New("namespace should be supplied when deleting pvc")
	}
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	c, err := GetKubeClient(path)
	if err != nil {
		return err
	}

	toolsPvcLabel := DefaultAirflowPVCLabelKey + "=" + DefaultToolsNamePrefix + DefaultToolAirflowName
	err = c.CoreV1().PersistentVolumeClaims(namespace).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: toolsPvcLabel})
	if err != nil {
		return err
	}
	toolsPvcLabel = DefaultZookeeperPVCLabelKey + "=" + DefaultToolsNamePrefix + DefaultToolZookeeperName
	err = c.CoreV1().PersistentVolumeClaims(namespace).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: toolsPvcLabel})
	if err != nil {
		return err
	}
	return nil
}

func (t *toolsCmd) genSupersetSecretFile() error {
	file, err := os.Create(DefaultToolSupersetSecretFile)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Error: %v \n", err)
		}
	}(file)
	data := []byte("SECRET_KEY='7frRUd8B0QXf23P1BUMlLdqdtz0UZMEs1dSyWiBMMs9Q7AZAVFjwfIr7'")
	_, err = file.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (t *toolsCmd) createDorisDatabase(ip string, port int32, user string, password string) error {
	connStr := fmt.Sprintf("%s@tcp(%s:%d)/", user, ip, port)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		fmt.Printf("Error:%v\n", err)
		return err
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + DefaultDorisDatabaseName)
	if err != nil {
		fmt.Printf("Error:%v\n", err)
		return err
	}
	return nil
}

func (t *toolsCmd) genSupersetDataSourcesFile() error {
	thriftIP, thriftPort := GetThriftIpAndPort(t.nineName, t.ns)
	if thriftIP == "" || thriftPort == 0 {
		return errors.New("invalid Thrift Access Info")
	}

	data := map[string][]DatabasesConnection{
		"databases": {
			{
				AllowFileUpload: true,
				AllowCTAS:       true,
				AllowCVAS:       true,
				DatabaseName:    "default",
				Extra:           "{\r\n    \"metadata_params\": {},\r\n    \"engine_params\": {},\r\n    \"metadata_cache_timeout\": {},\r\n    \"schemas_allowed_for_file_upload\": []\r\n}",
				SqlAlchemyURI:   fmt.Sprintf("hive://%s@%s:%d", DefaultKyuubiUserName, thriftIP, thriftPort),
				Tables:          []string{},
			},
		},
	}
	dorisIP, dorisPort := GetDorisIpAndPort(t.nineName, t.ns)
	if dorisIP != "" && dorisPort != 0 {
		if err := t.createDorisDatabase(dorisIP, dorisPort, DefaultDorisAdminUser, DefaultDorisAdminPassword); err != nil {
			return err
		}
		data["databases"] = append(data["databases"], DatabasesConnection{
			AllowFileUpload: true,
			AllowCTAS:       true,
			AllowCVAS:       true,
			DatabaseName:    DefaultDorisDatabaseName,
			Extra:           "{\r\n    \"metadata_params\": {},\r\n    \"engine_params\": {},\r\n    \"metadata_cache_timeout\": {},\r\n    \"schemas_allowed_for_file_upload\": []\r\n}",
			SqlAlchemyURI:   fmt.Sprintf("mysql://%s@%s:%d", DefaultDorisAdminUser, dorisIP, dorisPort),
			Tables:          []string{},
		})
	}
	yamlData, err := yaml.Marshal(&data)
	if err != nil {
		return err
	}
	file, err := os.Create(DefaultToolSupersetSDataSourcesFile)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Error: %v \n", err)
		}
	}(file)
	_, err = file.Write(yamlData)
	if err != nil {
		return err
	}
	return nil
}

func (t *toolsCmd) genSupersetParameters(relName string, parameters []string) []string {
	if err := t.genSupersetSecretFile(); err != nil {
		fmt.Printf("Error: %s \n", err.Error())
		return []string{""}
	}
	if err := t.genSupersetDataSourcesFile(); err != nil {
		fmt.Printf("Error: %s \n", err.Error())
		return []string{""}
	}
	params := append(parameters, []string{"--set", fmt.Sprintf("fullnameOverride=%s", relName)}...)
	params = append(params, []string{"--set", fmt.Sprintf("supersetNode.connections.redis_host=%s", DefaultRedisSVCName)}...)
	params = append(params, []string{"--set", fmt.Sprintf("supersetNode.connections.db_user=%s", DefaultToolSupersetDBUser)}...)
	params = append(params, []string{"--set", fmt.Sprintf("supersetNode.connections.db_pass=%s", DefaultToolSupersetDBPwd)}...)
	params = append(params, []string{"--set", fmt.Sprintf("supersetNode.connections.db_name=%s", DefaultToolSupersetDBName)}...)
	params = append(params, []string{"--set", fmt.Sprintf("supersetNode.connections.db_host=%s", t.nineName+DefaultPGRWSVCNameSuffix)}...)
	params = append(params, []string{"--set", fmt.Sprintf("service.type=%s", DefaultToolSupersetSvcType)}...)
	params = append(params, []string{"--set-file", fmt.Sprintf("configOverrides.secret=%s", DefaultToolSupersetSecretFile)}...)
	params = append(params, []string{"--set-file", fmt.Sprintf("extraConfigs.import_datasources=%s", DefaultToolSupersetSDataSourcesFile)}...)
	params = append(params, []string{"--set", "redis.enabled=false"}...)
	params = append(params, []string{"--set", "postgresql.enabled=false"}...)
	params = append(params, []string{"--set", "extraEnv.TALISMAN_ENABLED=\"False\""}...)
	return params
}

func (t *toolsCmd) genZookeeperParameters(relName string, parameters []string) []string {
	params := append(parameters, []string{"--set", fmt.Sprintf("fullnameOverride=%s", relName)}...)
	params = append(params, []string{"--set", fmt.Sprintf("persistence.storageClass=%s", t.storagepool)}...)
	params = append(params, []string{"--set", fmt.Sprintf("replicaCount=%d", DefaultZookeeperReplicas)}...)
	params = append(params, []string{"--set", "podAntiAffinityPreset=hard"}...)
	return params
}

func (t *toolsCmd) genNifiParameters(relName string, parameters []string) []string {
	var nodePortIp string
	if DefaultAccessHost != "" {
		nodePortIp = DefaultAccessHost
	} else {
		path, _ := rootCmd.Flags().GetString(kubeconfig)
		var err error
		nodePortIp, err = GetKubeHost(path)
		if err != nil {
			fmt.Printf("cannot get host ip for the nifi web access,err:%s,you can specify the host ip through --access-host\n", err.Error())
		}
	}
	params := append(parameters, []string{"--set", "fullnameOverride=" + relName}...)
	params = append(params, []string{"--set", "auth.enabled=false"}...)
	params = append(params, []string{"--set", fmt.Sprintf("master.persistence.storageClass=%s", t.storagepool)}...)
	params = append(params, []string{"--set", fmt.Sprintf("service.type=%s", DefaultToolNifiSvcType)}...)
	params = append(params, []string{"--set", fmt.Sprintf("service.nodePort=%d", DefaultToolNifiSvcNodePort)}...)
	params = append(params, []string{"--set", fmt.Sprintf("properties.webProxyHost=%s:%d", nodePortIp, DefaultToolNifiSvcNodePort)}...)
	params = append(params, []string{"--set", fmt.Sprintf("zookeeper.url=%s", t.zkSvcName)}...)
	params = append(params, []string{"--set", fmt.Sprintf("auth.singleUser.username=%s", DefaultToolNifiUserName)}...)
	params = append(params, []string{"--set", fmt.Sprintf("auth.singleUser.password=%s", DefaultToolNifiUserPWD)}...)
	params = append(params, []string{"--set", fmt.Sprintf("sidecar.tag=%s", DefaultToolNifiSideCarTag)}...)
	params = append(params, []string{"--set", "zookeeper.enabled=false"}...)
	return params
}

func (t *toolsCmd) genAirflowParameters(relName string, parameters []string) []string {
	params := append(parameters, []string{"--set", fmt.Sprintf("fullnameOverride=%s", relName)}...)
	params = append(params, []string{"--set", fmt.Sprintf("data.brokerUrl=redis://%s", DefaultRedisSVCName)}...)
	params = append(params, []string{"--set", fmt.Sprintf("data.metadataConnection.user=%s", DefaultToolAirflowDBUser)}...)
	params = append(params, []string{"--set", fmt.Sprintf("data.metadataConnection.pass=%s", DefaultToolAirflowDBPwd)}...)
	params = append(params, []string{"--set", fmt.Sprintf("data.metadataConnection.db=%s", DefaultToolAirflowDBName)}...)
	params = append(params, []string{"--set", fmt.Sprintf("data.metadataConnection.host=%s", t.nineName+DefaultPGRWSVCNameSuffix)}...)
	params = append(params, []string{"--set", fmt.Sprintf("images.airflow.repository=%s", DefaultToolAirflowRepository)}...)
	params = append(params, []string{"--set", fmt.Sprintf("images.airflow.tag=%s", DefaultToolAirflowTag)}...)
	params = append(params, []string{"--set", fmt.Sprintf("webserverSecretKey=%s", DefaultToolAirflowWebServerSecretKey)}...)
	params = append(params, []string{"--set", fmt.Sprintf("webserver.service.type=%s", DefaultToolAirflowSvcType)}...)
	params = append(params, []string{"--set", fmt.Sprintf("logs.persistence.size=%s", DefaultToolAirflowDiskSize)}...)
	params = append(params, []string{"--set", fmt.Sprintf("workers.persistence.size=%s", DefaultToolAirflowDiskSize)}...)
	params = append(params, []string{"--set", fmt.Sprintf("triggerer.persistence.size=%s", DefaultToolAirflowDiskSize)}...)
	params = append(params, []string{"--set", fmt.Sprintf("workers.persistence.storageClassName=%s", t.storagepool)}...)
	params = append(params, []string{"--set", fmt.Sprintf("triggerer.persistence.storageClassName=%s", t.storagepool)}...)
	params = append(params, []string{"--set", fmt.Sprintf("dags.persistence.storageClassName=%s", t.storagepool)}...)
	params = append(params, []string{"--set", "statsd.enabled=false"}...)
	params = append(params, []string{"--set", "redis.enabled=false"}...)
	params = append(params, []string{"--set", "statsd.enabled=false"}...)
	params = append(params, []string{"--set", "postgresql.enabled=false"}...)
	return params
}

func (t *toolsCmd) genRedisParameters(relName string, parameters []string) []string {
	params := append(parameters, []string{"--set", "fullnameOverride=" + relName}...)
	params = append(params, []string{"--set", fmt.Sprintf("storage.className=%s", t.storagepool)}...)
	return params
}

func (t *toolsCmd) createDatabase(tool string) error {
	pgIP, pgPort := GetPostgresIpAndPort(t.nineName, t.ns)
	if pgIP == "" || pgPort == 0 {
		return errors.New("invalid Postgres Access Info")
	}
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", pgIP, pgPort, "postgres", "", "")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Printf("Error:%v\n", err)
		return err
	}
	defer db.Close()
	var dbUser, dbName, dbPWD string
	switch tool {
	case DefaultToolAirflowName:
		dbUser = DefaultToolAirflowDBUser
		dbPWD = DefaultToolAirflowDBPwd
		dbName = DefaultToolAirflowName
	case DefaultToolSupersetName:
		dbUser = DefaultToolSupersetDBUser
		dbPWD = DefaultToolSupersetDBPwd
		dbName = DefaultToolSupersetName
	}

	_, err = db.Exec("CREATE USER " + dbUser + " WITH PASSWORD '" + dbPWD + "'")
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		fmt.Printf("Error:%v\n", err)
		return err
	}
	_, err = db.Exec("CREATE DATABASE " + dbName + " WITH OWNER " + dbUser)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		fmt.Printf("Error:%v\n", err)
		return err
	}
	return nil
}

func (t *toolsCmd) installRedis(parameters []string) error {
	relName := DefaultToolsNamePrefix + DefaultToolRedisName
	err := HelmInstallWithParameters(relName, "", t.chartPath, DefaultToolRedisName, DefaultToolsChartList[DefaultToolRedisName], t.ns, t.genRedisParameters(relName, parameters)...)
	if err != nil {
		return err
	}
	return nil
}

func (t *toolsCmd) installAirflow(parameters []string) error {
	err := t.installRedis(parameters)
	if err != nil {
		return err
	}
	err = t.createDatabase(DefaultToolAirflowName)
	if err != nil {
		return err
	}
	relName := DefaultToolsNamePrefix + DefaultToolAirflowName
	err = HelmInstallWithParameters(relName, "", t.chartPath, DefaultToolAirflowName, DefaultToolsChartList[DefaultToolAirflowName], t.ns, t.genAirflowParameters(relName, parameters)...)
	if err != nil {
		return err
	}
	return nil
}

func (t *toolsCmd) installSuperset(parameters []string) error {
	err := t.installRedis(parameters)
	if err != nil {
		return err
	}
	err = t.createDatabase(DefaultToolSupersetName)
	if err != nil {
		return err
	}
	relName := DefaultToolsNamePrefix + DefaultToolSupersetName
	err = HelmInstallWithParameters(relName, "", t.chartPath, DefaultToolSupersetName, DefaultToolsChartList[DefaultToolSupersetName], t.ns, t.genSupersetParameters(relName, parameters)...)
	if err != nil {
		return err
	}
	return nil
}

func (t *toolsCmd) checkZookeeperCluster() bool {
	epName := fmt.Sprintf("%s%s", t.nineName, DefaultZookeeperHLSVCNameSuffix)
	err, ready, _ := CheckEndpointsReady(epName, t.ns, DefaultZookeeperReplicas)
	if err != nil && !k8serrors.IsNotFound(err) {
		return false
	}
	if k8serrors.IsNotFound(err) {
		return false
	}
	return ready
}

func (t *toolsCmd) installZookeeper(parameters []string) error {
	if t.checkZookeeperCluster() {
		fmt.Printf("A zookeeper cluster exists,no need to install!\n")
		t.zkSvcName = fmt.Sprintf("%s%s", t.nineName, DefaultZookeeperHLSVCNameSuffix)
		return nil
	}
	relName := DefaultToolsNamePrefix + DefaultToolZookeeperName
	err := HelmInstallWithParameters(relName, "", t.chartPath, DefaultToolZookeeperName, DefaultToolsChartList[DefaultToolZookeeperName], t.ns, t.genZookeeperParameters(relName, parameters)...)
	if err != nil {
		return err
	}
	t.zkSvcName = DefaultZookeeperSVCName
	return nil
}

func (t *toolsCmd) installNifi(parameters []string) error {
	err := t.installZookeeper(parameters)
	if err != nil {
		return err
	}
	relName := DefaultToolsNamePrefix + DefaultToolNifiName
	err = HelmInstallWithParameters(relName, "", t.chartPath, DefaultToolNifiName, DefaultToolsChartList[DefaultToolNifiName], t.ns, t.genNifiParameters(relName, parameters)...)
	if err != nil {
		return err
	}
	return nil
}

func (t *toolsCmd) install(parameters []string) error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	nc, err := GetNineInfraClient(path)
	if err != nil {
		return err
	}
	listClusters, err := nc.NineinfraV1alpha1().NineClusters(t.ns).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	if len(listClusters.Items) == 0 {
		return errors.New("NineCluster not found in namespace:" + t.ns)
	}
	t.nineName = listClusters.Items[0].Name

	for _, v := range t.toolkitArgs {
		switch v {
		case DefaultToolAirflowName:
			err = t.installAirflow(parameters)
		case DefaultToolSupersetName:
			err = t.installSuperset(parameters)
		case DefaultToolNifiName:
			err = t.installNifi(parameters)
		case DefaultToolRedisName:
			err = t.installRedis(parameters)
		case DefaultToolZookeeperName:
			err = t.installZookeeper(parameters)
		}
	}
	return err
}

func (t *toolsCmd) uninstall(parameters []string) error {
	flags := strings.Join(parameters, " ")
	for _, v := range t.toolkitArgs {
		relName := DefaultToolsNamePrefix + v
		err := HelmUnInstall(relName, t.ns, flags)
		if err != nil {
			fmt.Printf("Error: %v \n", err)
			return err
		}
	}

	if t.deletePVC {
		if err := t.deleteToolsPVC(t.ns); err != nil {
			return err
		}
	}
	return nil
}

func (t *toolsCmd) list() error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	nclient, err := GetNineInfraClient(path)
	if err != nil {
		return err
	}

	clusters, err := nclient.NineinfraV1alpha1().NineClusters("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	PrintToolList(clusters)
	return nil
}

// run initializes local config and installs the tools to Kubernetes cluster.
func (t *toolsCmd) run() error {

	path, _ := rootCmd.Flags().GetString(kubeconfig)

	var parameters []string
	if path != "" {
		parameters = append(parameters, []string{"--kubeconfig", path}...)
	}

	switch t.subCommand {
	case "install":
		err := t.install(parameters)
		if err != nil {
			return err
		}
	case "uninstall":
		err := t.uninstall(parameters)
		if err != nil {
			return err
		}
	case "list":
		err := t.list()
		if err != nil {
			return err
		}
	}

	return nil
}
