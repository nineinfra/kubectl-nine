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
	onekeyDesc    = `'onekey' command allows you to complete everything with the NineCluster you want with just one click.`
	onekeyExample = `1. Install everything for the NineCluster
   $ kubectl nine onekey --command=install --namespace=ns

2. Uninstall everything of the NineCluster from a namespace
   $ kubectl nine onekey --command=uninstall --namespace=ns`
)

var (
	onekeySubCommandList = "install,uninstall,list"
)

type onekeyCmd struct {
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

func newOnekeyCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &onekeyCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "onekey",
		Short:   "Allows you to complete everything with the NineCluster you want with just one click.",
		Long:    onekeyDesc,
		Example: onekeyExample,
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
	f.StringVarP(&c.subCommand, "command", "c", "", fmt.Sprintf("command for tools,%s are supported now", onekeySubCommandList))
	f.StringVar(&c.jdbcQuery, "jdbc-query", "", "jdbc query for the etl")
	f.StringVar(&c.partitionColumn, "partition-column", "", "the column name for parallelism's partition")
	f.StringVar(&c.dagsPath, "dags-path", "", "local path of the airflow dags")
	f.StringVar(&c.sinkTable, "sink-table", "test", "name of the sink table")
	f.StringVarP(&c.ns, "namespace", "n", "", "k8s namespace for the NineCluster")
	f.BoolVar(&DEBUG, "debug", false, "print debug information")
	return cmd
}

func (o *onekeyCmd) validate(args []string) error {
	if !strings.Contains(onekeySubCommandList, o.subCommand) {
		return fmt.Errorf("unsupported subcommand %s, only %s supported", o.subCommand, onekeySubCommandList)
	}

	return nil
}

func (o *onekeyCmd) formatValue(value interface{}) string {
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

func (o *onekeyCmd) structToKeyValueString(s interface{}) string {
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

func (o *onekeyCmd) pg2hdfsConf2String(conf *pg2hdfsConf) (string, error) {
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

func (o *onekeyCmd) pg2dorisConf2String(conf *pg2dorisConf) (string, error) {
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

func (o *onekeyCmd) constructConfigmap(cluster *nineinfrav1alpha1.NineCluster, suffix string, conf string) (*corev1.ConfigMap, error) {
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

func (o *onekeyCmd) deleteConfigmap(cluster *nineinfrav1alpha1.NineCluster, suffix string) error {
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

func (o *onekeyCmd) createConfigmap(cluster *nineinfrav1alpha1.NineCluster, suffix string, conf string) error {
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

func (o *onekeyCmd) getHdfsConf(cluster *nineinfrav1alpha1.NineCluster) (string, string, error) {
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

func (o *onekeyCmd) uploadFileToAirflow(filename string, cluster *nineinfrav1alpha1.NineCluster) error {
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

func (o *onekeyCmd) createEtlConfigmap(cluster *nineinfrav1alpha1.NineCluster) error {
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
					// if set to "",seatunnel will retuen no data.
					Partition_column:             o.partitionColumn,
					Connection_check_timeout_sec: 30,
				},
			},
			Sink: map[string]stHdfsSink{
				"HdfsFile": {
					DefaultFS:        coreSiteMap["fs.defaultFS"],
					Path:             "/nineinfra/datahouse/seatunnel",
					Tmp_path:         "/nineinfra/datahouse/tmp",
					Hdfs_site_path:   fmt.Sprintf("%s/%s", "/opt/spark/conf", DefaultHdfsSiteFileName),
					File_format_type: "text",
					Field_delimiter:  ",",
					Row_delimiter:    "\\n",
					Compress_codec:   "none",
					// if set to zero, seatunnel will write a file per row
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

func (o *onekeyCmd) deleteEtlConfigmap() error {
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

func (o *onekeyCmd) uninstall(parameters []string) error {
	err := o.deleteEtlConfigmap()
	if err != nil {
		return err
	}
	return nil
}

func (o *onekeyCmd) install(parameters []string) error {
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
func (o *onekeyCmd) run() error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)

	var parameters []string
	if path != "" {
		parameters = append(parameters, []string{"--kubeconfig", path}...)
	}

	switch o.subCommand {
	case "install":
		err := o.install(parameters)
		if err != nil {
			return err
		}
	case "uninstall":
		err := o.uninstall(parameters)
		if err != nil {
			return err
		}
	}

	return nil
}
