package cmd

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/cloudnative-pg/client/clientset/versioned/scheme"
	nineinfrav1alpha1 "github.com/nineinfra/nineinfra/api/v1alpha1"
	"github.com/spf13/cobra"
	"io"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"strings"
)

const (
	etlDesc    = `'etl' command help you to complete the ETL-related configurations with the NineCluster.`
	etlExample = `1. Install everything for the NineCluster
   $ kubectl nine etl --command=install --namespace=ns

2. Uninstall everything of the NineCluster from a namespace
   $ kubectl nine etl --command=uninstall --namespace=ns`
)

var (
	etlSourceList     = "postgresql"
	etlSinkList       = "hdfs,minio,doris"
	etlSubCommandList = "configure,update,clear"
)

type stJdbcSource struct {
	Url                          string `yaml:"url"`
	Driver                       string `yaml:"driver"`
	User                         string `yaml:"user"`
	Password                     string `yaml:"password"`
	Query                        string `yaml:"query"`
	Connection_check_timeout_sec int    `yaml:"connection_check_timeout_sec"`
	Partition_column             string `yaml:"partition_column"`
	//Partition_lower_bound        big.Float         `yaml:"partition_lower_bound"`
	//Partition_upper_bound        big.Float         `yaml:"partition_upper_bound"`
	Partition_num int               `yaml:"partition_num"`
	Fetch_size    int               `yaml:"fetch_size"`
	Properties    map[string]string `yaml:"properties"`
}

type stSqlTransform struct {
	Source_table_name string `yaml:"source_table_name"`
	Result_table_name string `yaml:"result_table_name"`
	Query             string `yaml:"query"`
}

type stHdfsSink struct {
	DefaultFS                        string   `yaml:"fs.defaultFS"`
	Path                             string   `yaml:"path"`
	Tmp_path                         string   `yaml:"tmp_path"`
	Hdfs_site_path                   string   `yaml:"hdfs_site_path"`
	Custom_filename                  bool     `yaml:"custom_filename"`
	File_name_expression             string   `yaml:"file_name_expression"`
	Filename_time_format             string   `yaml:"filename_time_format"`
	File_format_type                 string   `yaml:"file_format_type"`
	Field_delimiter                  string   `yaml:"field_delimiter"`
	Row_delimiter                    string   `yaml:"row_delimiter"`
	Have_partition                   bool     `yaml:"have_partition"`
	Partition_by                     []string `yaml:"partition_by"`
	Partition_dir_expression         string   `yaml:"partition_dir_expression"`
	Is_partition_field_write_in_file bool     `yaml:"is_partition_field_write_in_file"`
	Sink_columns                     []string `yaml:"sink_columns"`
	Is_enable_transaction            bool     `yaml:"is_enable_transaction"`
	Batch_size                       int      `yaml:"batch_size"`
	Compress_codec                   string   `yaml:"compress_codec"`
	Krb5_path                        string   `yaml:"krb5_path"`
	Kerberos_principal               string   `yaml:"kerberos_principal"`
	Kerberos_keytab_path             string   `yaml:"kerberos_keytab_path"`
	Max_rows_in_memory               int      `yaml:"max_rows_in_memory"`
	Sheet_name                       string   `yaml:"sheet_name"`
}

type stDorisSink struct {
	Fenodes                        string            `yaml:"fenodes"`
	Query_port                     int               `yaml:"query-port"`
	Username                       string            `yaml:"username"`
	Password                       string            `yaml:"password"`
	Database                       string            `yaml:"database"`
	Table                          string            `yaml:"table"`
	Identifier                     string            `yaml:"table.identifier"`
	Label_prefix                   string            `yaml:"sink.label-prefix"`
	Enable_2pc                     bool              `yaml:"sink.enable-2pc"`
	Enable_delete                  bool              `yaml:"sink.enable-delete"`
	Check_interval                 int               `yaml:"sink.check-interval"`
	Max_retries                    int               `yaml:"sink.max-retries"`
	Buffer_size                    int               `yaml:"sink.buffer-size"`
	Buffer_count                   int               `yaml:"sink.buffer-count"`
	Batch_size                     int               `yaml:"sink.batch.size"`
	Needs_unsupported_type_casting bool              `yaml:"needs_unsupported_type_casting"`
	Schema_save_mode               string            `yaml:"schema_save_mode"`
	Data_save_mode                 string            `yaml:"data_save_mode"`
	Save_mode_create_template      string            `yaml:"save_mode_create_template"`
	Custom_sql                     string            `yaml:"custom_sql"`
	Config                         map[string]string `yaml:"doris.config"`
}

type pg2hdfsConf struct {
	Env       map[string]string         `yaml:"env"`
	Source    map[string]stJdbcSource   `yaml:"source"`
	Transform map[string]stSqlTransform `yaml:"transform"`
	Sink      map[string]stHdfsSink     `yaml:"sink"`
}

type pg2dorisConf struct {
	Env       map[string]string         `yaml:"env"`
	Source    map[string]stJdbcSource   `yaml:"source"`
	Transform map[string]stSqlTransform `yaml:"transform"`
	Sink      map[string]stDorisSink    `yaml:"sink"`
}

type XmlProperty struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type XmlConfiguration struct {
	XmlName    xml.Name      `xml:"configuration"`
	Properties []XmlProperty `xml:"property"`
}

type etlCmd struct {
	out             io.Writer
	errOut          io.Writer
	subCommand      string
	ns              string
	nineName        string
	dagsPath        string
	sinkTable       string
	jdbcQuery       string
	partitionColumn string
}

func newetlCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &etlCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "etl",
		Short:   "Helps you to complete the ETL-related configurations with the NineCluster.",
		Long:    etlDesc,
		Example: etlExample,
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
	f.StringVarP(&c.subCommand, "command", "c", "", fmt.Sprintf("command for tools,%s are supported now", etlSubCommandList))
	f.StringVar(&c.jdbcQuery, "jdbc-query", "", "jdbc query for the etl")
	f.StringVar(&c.partitionColumn, "partition-column", "", "the column name for parallelism's partition")
	f.StringVar(&c.dagsPath, "dags-path", "", "local path of the airflow dags")
	f.StringVar(&c.sinkTable, "sink-table", "test", "name of the sink table")
	f.StringVarP(&c.ns, "namespace", "n", "", "k8s namespace for the NineCluster")
	f.BoolVar(&DEBUG, "debug", false, "print debug information")
	return cmd
}

func (o *etlCmd) validate(args []string) error {
	if !strings.Contains(etlSubCommandList, o.subCommand) {
		return fmt.Errorf("unsupported subcommand %s, only %s supported", o.subCommand, etlSubCommandList)
	}

	return nil
}

func (o *etlCmd) formatValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf(`"%v"`, v)
	case map[string]string:
		var sb strings.Builder
		sb.WriteString("{")
		for k, v := range value.(map[string]string) {
			sb.WriteString(fmt.Sprintf(`"%s":"%s",`, k, v))
		}
		sb.WriteString("}")
		return sb.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (o *etlCmd) structToKeyValueString(s interface{}) string {
	v := reflect.ValueOf(s)
	t := v.Type()
	var result []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i).Interface()

		tag := field.Tag.Get("yaml")
		if tag != "" {
			fieldValue := o.formatValue(value)
			if !strings.EqualFold(fieldValue, "\"\"") && !strings.EqualFold(fieldValue, "0") {
				result = append(result, fmt.Sprintf("    %s=%s", tag, fieldValue))
			}
		}
	}
	return strings.Join(result, "\n")
}

//func mapToStruct(m map[string]interface{}) etlCmd {
//	o := etlCmd{}
//	val := reflect.ValueOf(&o).Elem()
//	typ := val.Type()
//
//	for i := 0; i < val.NumField(); i++ {
//		field := typ.Field(i)
//		tag := field.Tag.Get("yaml")
//		if tag == "" {
//			continue
//		}
//
//		if v, ok := m[tag]; ok {
//			f := val.Field(i)
//			if f.CanSet() {
//				f.Set(reflect.ValueOf(v))
//			}
//		}
//	}
//
//	return o
//}

func (o *etlCmd) pg2hdfsConf2String(conf *pg2hdfsConf) (string, error) {
	var sb strings.Builder
	//env
	sb.WriteString("env{\n")
	for k, v := range conf.Env {
		sb.WriteString("  ")
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(v)
		sb.WriteString("\n")
	}
	sb.WriteString("}\n")

	//source
	sb.WriteString("source{\n")
	for k, v := range conf.Source {
		sb.WriteString("  ")
		sb.WriteString(k)
		sb.WriteString("{\n")
		sb.WriteString(o.structToKeyValueString(v))
		sb.WriteString("\n  }\n")
	}
	sb.WriteString("}\n")

	//transform
	sb.WriteString("transform{\n")
	for k, v := range conf.Transform {
		sb.WriteString("  ")
		sb.WriteString(k)
		sb.WriteString("{\n")
		sb.WriteString(o.structToKeyValueString(v))
		sb.WriteString("\n  }\n")
	}
	sb.WriteString("}\n")

	//sink
	sb.WriteString("sink{\n")
	for k, v := range conf.Sink {
		sb.WriteString("  ")
		sb.WriteString(k)
		sb.WriteString("{\n")
		sb.WriteString(o.structToKeyValueString(v))
		sb.WriteString("\n  }\n")
	}
	sb.WriteString("}\n")
	return sb.String(), nil
}

func (o *etlCmd) pg2dorisConf2String(conf *pg2dorisConf) (string, error) {
	var sb strings.Builder
	//env
	sb.WriteString("env{\n")
	for k, v := range conf.Env {
		sb.WriteString("  ")
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(v)
		sb.WriteString("\n")
	}
	sb.WriteString("}\n")

	//source
	sb.WriteString("source{\n")
	for k, v := range conf.Source {
		sb.WriteString("  ")
		sb.WriteString(k)
		sb.WriteString("{\n")
		sb.WriteString(o.structToKeyValueString(v))
		sb.WriteString("\n  }\n")
	}
	sb.WriteString("}\n")

	//transform
	sb.WriteString("transform{\n")
	for k, v := range conf.Transform {
		sb.WriteString("  ")
		sb.WriteString(k)
		sb.WriteString("{\n")
		sb.WriteString(o.structToKeyValueString(v))
		sb.WriteString("\n  }\n")
	}
	sb.WriteString("}\n")

	//sink
	sb.WriteString("sink{\n")
	for k, v := range conf.Sink {
		sb.WriteString("  ")
		sb.WriteString(k)
		sb.WriteString("{\n")
		sb.WriteString(o.structToKeyValueString(v))
		sb.WriteString("\n  }\n")
	}
	sb.WriteString("}\n")
	return sb.String(), nil
}

func (o *etlCmd) constructConfigmap(cluster *nineinfrav1alpha1.NineCluster, suffix string, conf string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NineResourceName(cluster.Name, suffix),
			Namespace: cluster.Namespace,
			Labels:    NineResourceLabels(cluster),
		},
		Data: map[string]string{
			fmt.Sprintf("%s.conf", NineResourceName(cluster.Name, suffix)): conf,
		},
	}

	metav1.AddToGroupVersion(scheme.Scheme, nineinfrav1alpha1.GroupVersion)
	utilruntime.Must(nineinfrav1alpha1.AddToScheme(scheme.Scheme))

	if err := ctrl.SetControllerReference(cluster, cm, scheme.Scheme); err != nil {
		return cm, err
	}
	return cm, nil
}

func (o *etlCmd) deleteConfigmap(cluster *nineinfrav1alpha1.NineCluster, suffix string) error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	c, err := GetKubeClient(path)
	if err != nil {
		return err
	}
	err = c.CoreV1().ConfigMaps(o.ns).Delete(context.TODO(), NineResourceName(cluster.Name, suffix), metav1.DeleteOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (o *etlCmd) createConfigmap(cluster *nineinfrav1alpha1.NineCluster, suffix string, conf string) error {
	desiredCm, err := o.constructConfigmap(cluster, suffix, conf)
	if err != nil {
		return err
	}
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	c, err := GetKubeClient(path)
	if err != nil {
		return err
	}
	existsCm, err := c.CoreV1().ConfigMaps(o.ns).Get(context.TODO(), desiredCm.Name, metav1.GetOptions{})
	if err != nil && k8serrors.IsNotFound(err) {
		fmt.Printf("Creating a new ConfigMap with name:%s\n", desiredCm.Name)
		_, err := c.CoreV1().ConfigMaps(o.ns).Create(context.TODO(), desiredCm, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		fmt.Printf("Updating an existing ConfigMap name:%s\n", existsCm.Name)
		existsCm.Data = desiredCm.Data
		_, err := c.CoreV1().ConfigMaps(o.ns).Update(context.TODO(), existsCm, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *etlCmd) getHdfsConf(cluster *nineinfrav1alpha1.NineCluster) (string, string, error) {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	c, err := GetKubeClient(path)
	if err != nil {
		return "", "", err
	}
	hdfsCm, err := c.CoreV1().ConfigMaps(o.ns).Get(context.TODO(), NineResourceName(cluster.Name, "hdfs", "config"), metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}

	return hdfsCm.Data["core-site.xml"], hdfsCm.Data["hdfs-site.xml"], nil
}

func (o *etlCmd) uploadFileToAirflow(filename string, cluster *nineinfrav1alpha1.NineCluster) error {
	podNames, err := GetAirflowPodNames(cluster.Name, "scheduler", cluster.Namespace)
	if err != nil {
		return err
	}
	_, _, err = runCommand("kubectl", "cp", filename, fmt.Sprintf("%s:%s/%s", podNames[0], DefaultAirflowDagsPath, filename), "-n", cluster.Namespace)
	if err != nil {
		return err
	}
	return nil
}

func (o *etlCmd) createEtlConfigmap(cluster *nineinfrav1alpha1.NineCluster) error {
	storageType, err := GetNineClusterStorageType(o.nineName, o.ns)
	if err != nil {
		return err
	}
	pgIP, pgPort := GetPostgresIpAndPort(o.nineName, o.ns)
	if pgIP == "" || pgPort == 0 {
		return errors.New("invalid Postgres Access Info")
	}

	switch storageType {
	case FeaturesStorageValueMinio:
		//pg->minio
	case FeaturesStorageValueHdfs:
		//pg->hdfs
		coreSite, hdfsSite, err := o.getHdfsConf(cluster)
		if err != nil {
			return err
		}
		var xmlconf XmlConfiguration
		err = xml.Unmarshal([]byte(coreSite), &xmlconf)
		if err != nil {
			return err
		}
		coreSiteMap := make(map[string]string, 0)
		for _, prop := range xmlconf.Properties {
			coreSiteMap[prop.Name] = prop.Value
		}
		err = xml.Unmarshal([]byte(hdfsSite), &xmlconf)
		if err != nil {
			return err
		}
		hdfsSiteMap := make(map[string]string, 0)
		for _, prop := range xmlconf.Properties {
			hdfsSiteMap[prop.Name] = prop.Value
		}
		err = GenLocalFile(DefaultHdfsSiteFileName, []byte(hdfsSite))
		if err != nil {
			return err
		}
		err = o.uploadFileToAirflow(DefaultHdfsSiteFileName, cluster)
		if err != nil {
			return err
		}
		conf := &pg2hdfsConf{
			Env: map[string]string{
				// for spark on jdk17
				"spark.driver.defaultJavaOptions": "\"--add-exports java.base/sun.nio.ch=ALL-UNNAMED\"",
			},
			Source: map[string]stJdbcSource{
				"Jdbc": {
					Url:      fmt.Sprintf("jdbc:postgresql://%s:%d/%s", pgIP, pgPort, DefaultNineInfraDBName),
					Driver:   "org.postgresql.Driver",
					User:     DefaultNineInfraDBUser,
					Password: DefaultNineInfraDBPwd,
					Query:    o.jdbcQuery,
					// if set to "",seatunnel will return no data.
					Partition_column:             o.partitionColumn,
					Connection_check_timeout_sec: 30,
				},
			},
			Sink: map[string]stHdfsSink{
				"HdfsFile": {
					DefaultFS:        coreSiteMap["fs.defaultFS"],
					Path:             "/nineinfra/datahouse/ods",
					Tmp_path:         "/nineinfra/datahouse/tmp",
					Hdfs_site_path:   fmt.Sprintf("%s/%s", "/opt/spark/conf", DefaultHdfsSiteFileName),
					File_format_type: "text",
					Field_delimiter:  ",",
					Row_delimiter:    "\\n",
					Compress_codec:   "none",
					// if set to zero, seatunnel will write one file per row
					Batch_size:           10000000,
					Custom_filename:      true,
					File_name_expression: "${now}",
				},
			},
		}
		conf2String, err := o.pg2hdfsConf2String(conf)
		if err != nil {
			return err
		}
		fmt.Println(conf2String)
		err = o.createConfigmap(cluster, "pg2hdfs", conf2String)
		if err != nil {
			return err
		}
	}

	features, err := GetNineClusterFeatures(o.nineName, o.ns)
	if err != nil {
		return err
	}
	if features != nil {
		if value, ok := features[FeaturesOlapKey]; ok {
			if value == FeaturesOlapValueDoris {
				//pg->doris
				dorisIP, dorisPort := GetDorisIpAndPort(o.nineName, o.ns)
				if dorisIP == "" || dorisPort == 0 {
					return errors.New("get doris ip and port failed")
				}
				conf := &pg2dorisConf{
					Env: map[string]string{
						"parallelism": "10",
						"job.mode":    "BATCH",
					},
					Source: map[string]stJdbcSource{
						"Jdbc": {
							Url:      fmt.Sprintf("jdbc:postgresql://%s:%d/%s", pgIP, pgPort, DefaultNineInfraDBName),
							Driver:   "org.postgresql.Driver",
							User:     DefaultNineInfraDBUser,
							Password: DefaultNineInfraDBPwd,
							Query:    o.jdbcQuery,
						},
					},
					Sink: map[string]stDorisSink{
						"Doris": {
							Fenodes:  fmt.Sprintf("%s:%d", dorisIP, dorisPort),
							Username: DefaultDorisAdminUser,
							Password: DefaultDorisAdminPassword,
							Database: DefaultNineInfraDBName,
							Table:    "test",
						},
					},
				}
				conf2String, err := o.pg2dorisConf2String(conf)
				if err != nil {
					return err
				}
				fmt.Println(conf2String)
				err = o.createConfigmap(cluster, "pg2doris", conf2String)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (o *etlCmd) deleteEtlConfigmap() error {
	listClusters, err := GetNineCLusters(o.ns)
	if err != nil {
		return err
	}
	o.nineName = listClusters.Items[0].Name
	storageType, err := GetNineClusterStorageType(o.nineName, o.ns)
	if err != nil {
		return err
	}
	switch storageType {
	case FeaturesStorageValueMinio:
		//pg->minio
		err = o.deleteConfigmap(&listClusters.Items[0], "pg2minio")
		if err != nil {
			return err
		}
	case FeaturesStorageValueHdfs:
		//pg->hdfs
		err = o.deleteConfigmap(&listClusters.Items[0], "pg2hdfs")
		if err != nil {
			return err
		}
	}
	features, err := GetNineClusterFeatures(o.nineName, o.ns)
	if err != nil {
		return err
	}
	if features != nil {
		if value, ok := features[FeaturesOlapKey]; ok {
			if value == FeaturesOlapValueDoris {
				//pg->doris
				err = o.deleteConfigmap(&listClusters.Items[0], "pg2doris")
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (o *etlCmd) clear(parameters []string) error {
	err := o.deleteEtlConfigmap()
	if err != nil {
		return err
	}
	return nil
}

func (o *etlCmd) configure(parameters []string) error {
	listClusters, err := GetNineCLusters(o.ns)
	if err != nil {
		return err
	}
	o.nineName = listClusters.Items[0].Name
	err = o.createEtlConfigmap(&listClusters.Items[0])
	if err != nil {
		return err
	}
	if o.dagsPath != "" {
		err = o.uploadFileToAirflow(o.dagsPath, &listClusters.Items[0])
		if err != nil {
			return err
		}
	}

	return err
}

// run initializes local config and installs the tools to Kubernetes cluster.
func (o *etlCmd) run() error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)

	var parameters []string
	if path != "" {
		parameters = append(parameters, []string{"--kubeconfig", path}...)
	}

	switch o.subCommand {
	case "configure":
		err := o.configure(parameters)
		if err != nil {
			return err
		}
	case "clear":
		err := o.clear(parameters)
		if err != nil {
			return err
		}
	}

	return nil
}
