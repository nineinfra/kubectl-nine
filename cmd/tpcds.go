package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"strings"
)

const (
	tpcdsDesc    = `'tpcds' command run sql TPC-DS benchmark on the NineCluster`
	tpcdsExample = `1. Generate data
   $ kubectl nine tpcds c1 --namespace c1-ns -g -d tpcds_nine01

2. Generate data
   $ kubectl nine tpcds c1 --namespace c1-ns -g -d tpcds_nine01 -s 2 -p 20

3. TPC-DS benchmark
   $ kubectl nine tpcds c1 --namespace c1-ns -d tpcds_nine01 -i 3

4. TPC-DS benchmark with custom spark configs
   $ kubectl nine tpcds c1 --namespace c1-ns -d tpcds_nine01 -i 3 --num-executors=8 --executor-cores=4 --executor-memory=12`
)

type TPCDSOptions struct {
	Name            string
	NS              string
	GenData         bool
	DataBase        string
	TPCDSJar        string
	ScaleFactor     int
	Parallel        int
	Iterations      int
	Executors       int
	ExecutorMemory  int
	ExecutorCores   int
	DriverMemory    int
	DriverCores     int
	Queries         []string
	ResultsDir      string
	StorageClass    string
	ShuffleDiskSize int
	ShuffleDisks    int
	TTY             bool
	Stop            bool
	Force           bool
	DeployMode      string
	SparkUI         int
}

type tpcdsCmd struct {
	out          io.Writer
	errOut       io.Writer
	output       bool
	tpcdsOptions TPCDSOptions
}

func newClusterTPCDSCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &tpcdsCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "tpcds <NINECLUSTERNAME>",
		Short:   "Run sql TPC-DS benchmark on a NineCluster",
		Long:    tpcdsDesc,
		Example: tpcdsExample,
		Args: func(cmd *cobra.Command, args []string) error {
			return c.validate(args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := c.run(args)
			if err != nil {
				fmt.Println(GiveSuggestionsByError(err))
				return err
			}
			return nil
		},
	}
	cmd = DisableHelp(cmd)
	f := cmd.Flags()
	f.StringVarP(&c.tpcdsOptions.NS, "namespace", "n", "", "k8s namespace for this ninecluster")
	f.BoolVarP(&c.tpcdsOptions.GenData, "gendata", "g", false, "run data generator")
	f.StringVarP(&c.tpcdsOptions.DataBase, "database", "d", "default", "the database for TPC-DS")
	f.IntVarP(&c.tpcdsOptions.ScaleFactor, "scale", "s", 1, "the scale factor of TPC-DS")
	f.IntVarP(&c.tpcdsOptions.Parallel, "parallel", "p", c.tpcdsOptions.ScaleFactor*2, "the parallelism of Spark job")
	f.IntVarP(&c.tpcdsOptions.Iterations, "iterations", "i", 3, "the number of iterations to run")
	f.StringSliceVarP(&c.tpcdsOptions.Queries, "queries", "q", nil, "the queries of the TPC-DS,e.g. q1-v2.4,q2-v2.4 ")
	f.StringVarP(&c.tpcdsOptions.TPCDSJar, "jar", "j", fmt.Sprintf("kyuubi-tpcds_%s-%s.jar", DefaultScalaVersion, DefaultKyuubiVersion), "jar for TPC-DS")
	f.StringVarP(&c.tpcdsOptions.ResultsDir, "results-dir", "r", "s3a://nineinfra/datahouse/performance", "the dir to store benchmark results")
	f.IntVar(&c.tpcdsOptions.Executors, "num-executors", 0, "the number of the spark executors for the TPC-DS")
	f.IntVar(&c.tpcdsOptions.DriverCores, "driver-cores", 0, "the cores of the spark driver for the TPC-DS")
	f.IntVar(&c.tpcdsOptions.DriverMemory, "driver-memory", 0, "the memory of the spark driver for the TPC-DS")
	f.IntVar(&c.tpcdsOptions.ExecutorMemory, "executor-memory", 0, "the memory of the spark executor for the TPC-DS")
	f.IntVar(&c.tpcdsOptions.ExecutorCores, "executor-cores", 0, "the cores of the spark executor for the TPC-DS")
	f.StringVar(&c.tpcdsOptions.StorageClass, "storageclass", "directpv-min-io", "storageclass fo tpcds")
	f.IntVar(&c.tpcdsOptions.ShuffleDiskSize, "shuffle-disksize", 250, "shuffle disk size of executor")
	f.IntVar(&c.tpcdsOptions.ShuffleDisks, "shuffle-disks", 1, "shuffle disks of executor")
	f.StringVar(&c.tpcdsOptions.DeployMode, "deploy-mode", "client", "deploy mode of spark-submit")
	f.StringVar(&DefaultAccessHost, "access-host", "", "access host ip for out cluster access,such as web access")
	f.IntVar(&c.tpcdsOptions.SparkUI, "spark-ui", DefaultSparkUINodePort, "nodeport of spark UI")
	f.BoolVar(&c.tpcdsOptions.Stop, "stop", false, "stop and clean the running TPC-DS")
	f.BoolVar(&c.tpcdsOptions.Force, "force", false, "force to stop and clean the running TPC-DS")
	f.BoolVar(&c.tpcdsOptions.TTY, "tty", false, "enable tty")
	f.BoolVar(&DEBUG, "debug", false, "debug mode")
	return cmd
}

func (t *tpcdsCmd) validate(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("not enough parameters")
	}
	if !strings.Contains(ValidSparkDeployModeList, t.tpcdsOptions.DeployMode) {
		return fmt.Errorf("unsupported deploy mod %s, only %s supported", t.tpcdsOptions.DeployMode, ValidSparkDeployModeList)
	}
	t.tpcdsOptions.Name = args[0]
	return ValidateClusterArgs("tpcds", args)
}

func (t *tpcdsCmd) deleteSparkUIService() error {
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetKubeClient(path)
	if err != nil {
		return err
	}
	svcName := DefaultTPCDSPrefix + "-cluster"
	err = client.CoreV1().Services(t.tpcdsOptions.NS).Delete(context.TODO(), svcName, metav1.DeleteOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	} else if !k8serrors.IsNotFound(err) {
		fmt.Printf("Delete the spark-ui service %s successfully!\n", svcName)
	}
	svcName = DefaultTPCDSPrefix + "-client"
	err = client.CoreV1().Services(t.tpcdsOptions.NS).Delete(context.TODO(), svcName, metav1.DeleteOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	} else if !k8serrors.IsNotFound(err) {
		fmt.Printf("Delete the spark-ui service %s successfully!\n", svcName)
	}
	return nil
}

func (t *tpcdsCmd) updateSparkUIService() error {
	var svcName string
	var selector map[string]string
	if t.tpcdsOptions.DeployMode == SparkDeployModeCluster {
		svcName = DefaultTPCDSPrefix + "-cluster"
		selector = map[string]string{
			"cluster":    t.tpcdsOptions.Name,
			"app":        DefaultTPCDSAPP,
			"spark-role": "driver",
		}
	} else {
		svcName = DefaultTPCDSPrefix + "-client"
		selector = map[string]string{
			"cluster": t.tpcdsOptions.Name,
			"app":     "kyuubi",
		}
	}
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetKubeClient(path)
	if err != nil {
		return err
	}
	_, err = client.CoreV1().Services(t.tpcdsOptions.NS).Get(context.TODO(), svcName, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	if k8serrors.IsNotFound(err) {
		diseredSvc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      svcName,
				Namespace: t.tpcdsOptions.NS,
				Labels: map[string]string{
					"cluster": t.tpcdsOptions.Name,
					"app":     "kyuubi",
				},
			},
			Spec: corev1.ServiceSpec{
				Type: corev1.ServiceTypeNodePort,
				Ports: []corev1.ServicePort{
					{
						Name:     DefaultSparkUIName,
						Port:     DefaultSparkUIPort,
						NodePort: DefaultSparkUINodePort,
						TargetPort: intstr.IntOrString{
							Type:   intstr.Int,
							IntVal: int32(DefaultSparkUIPort),
						},
					},
				},
				Selector: selector,
			},
		}
		_, err := client.CoreV1().Services(t.tpcdsOptions.NS).Create(context.TODO(), diseredSvc, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *tpcdsCmd) stopRunningTPCDS(podName string) error {
	pid := GetCustomAppRunningPid(podName, t.tpcdsOptions.NS, DefaultTPCDSPrefix)
	if pid != "" {
		err := KillCustomAppRunningPid(podName, t.tpcdsOptions.NS, pid)
		if err != nil {
			return err
		}
		fmt.Printf("Kill the spark-submit process %s successfully!\n", pid)
	}
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetKubeClient(path)
	if err != nil {
		return err
	}
	if !t.tpcdsOptions.Force {
		selector := labels.Set(map[string]string{
			"cluster":    t.tpcdsOptions.Name,
			"app":        DefaultTPCDSAPP,
			"spark-role": "driver",
		}).AsSelector()
		podList, err := client.CoreV1().Pods(t.tpcdsOptions.NS).List(context.TODO(), metav1.ListOptions{LabelSelector: selector.String()})
		if len(podList.Items) > 0 {
			err = client.CoreV1().Pods(t.tpcdsOptions.NS).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: selector.String()})
			if err != nil {
				return err
			}
			fmt.Printf("Delete the spark driver pod %s and executor pods successfully!\n", podList.Items[0].Name)
		}
	} else {
		selector := labels.Set(map[string]string{
			"cluster": t.tpcdsOptions.Name,
			"app":     DefaultTPCDSAPP,
		}).AsSelector()
		podList, err := client.CoreV1().Pods(t.tpcdsOptions.NS).List(context.TODO(), metav1.ListOptions{LabelSelector: selector.String()})
		if len(podList.Items) > 0 {
			gracePeriodSeconds := int64(0)
			deletePolicy := metav1.DeletePropagationForeground
			err = client.CoreV1().Pods(t.tpcdsOptions.NS).DeleteCollection(context.TODO(),
				metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds, PropagationPolicy: &deletePolicy},
				metav1.ListOptions{LabelSelector: selector.String()})
			if err != nil {
				return err
			}
			fmt.Printf("Delete the spark driver pod and executor pods successfully!\n")
		}
		pvList, err := GetReleasedAndDeletePolicyPVList(client, DefaultTPCDSPrefix)
		if err != nil {
			return err
		}
		for _, pv := range pvList.Items {
			err = client.CoreV1().PersistentVolumes().Delete(context.TODO(), pv.Name, metav1.DeleteOptions{})
			if err != nil {
				return err
			}
			fmt.Printf("Delete the pv %s of the TPC-DS successfully!\n", pv.Name)
		}
		path, _ := rootCmd.Flags().GetString(kubeconfig)
		directpvClient, err := GetDirectPVClient(path)
		if err != nil {
			return err
		}
		directpvList, err := GetReadyDirectPVVolumes(directpvClient, t.tpcdsOptions.NS, DefaultTPCDSPrefix)
		if err != nil {
			return err
		}
		if directpvList != nil {
			for _, directpv := range directpvList.Items {
				directpv.SetFinalizers(nil)
				_, err = directpvClient.DirectPVVolumes().Update(context.TODO(), &directpv, metav1.UpdateOptions{})
				if err != nil {
					return err
				}
				err = directpvClient.DirectPVVolumes().Delete(context.TODO(), directpv.Name, metav1.DeleteOptions{})
				if err != nil && !k8serrors.IsNotFound(err) {
					return err
				}
				fmt.Printf("Delete the directpv %s of the TPC-DS successfully!\n", directpv.Name)
			}
		}
	}
	return nil
}

func (t *tpcdsCmd) checkTPCDSIsRunning(podName string) bool {
	pid := GetCustomAppRunningPid(podName, t.tpcdsOptions.NS, DefaultTPCDSPrefix)
	if pid != "" {
		return true
	}
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetKubeClient(path)
	if err != nil {
		return false
	}

	selector := labels.Set(map[string]string{
		"cluster": t.tpcdsOptions.Name,
		"app":     DefaultTPCDSAPP,
	}).AsSelector()
	podList, err := client.CoreV1().Pods(t.tpcdsOptions.NS).List(context.TODO(), metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return false
	}
	if len(podList.Items) != 0 {
		fmt.Printf("the TPC-DS is already running with some spark pods in executing,such as %s\n", podList.Items[0].Name)
		return true
	}
	pvList, err := GetReleasedAndDeletePolicyPVList(client, DefaultTPCDSPrefix)
	if err != nil {
		return false
	}
	if len(pvList.Items) != 0 {
		fmt.Printf("the TPC-DS is blocking with some pv in deleting,such as %s\n", pvList.Items[0].Name)
		return true
	}
	return false
}

func (t *tpcdsCmd) runTPCDS() error {
	podName, err := GetThriftPodName(t.tpcdsOptions.Name, t.tpcdsOptions.NS)
	if err != nil {
		return err
	}
	thriftIP, thriftPort := GetThriftIpAndPort(t.tpcdsOptions.Name, t.tpcdsOptions.NS)
	if thriftIP == "" || thriftPort == 0 {
		return errors.New("invalid Thrift Access Info")
	}
	config, err := GetKubeConfig()
	if err != nil {
		return err
	}

	if t.tpcdsOptions.Stop {
		err = t.deleteSparkUIService()
		if err != nil {
			return err
		}
		return t.stopRunningTPCDS(podName)
	}

	if t.checkTPCDSIsRunning(podName) {
		return fmt.Errorf("the TPC-DS is already exist")
	}

	if err := t.updateSparkUIService(); err != nil {
		fmt.Printf("Error: %v \n", err)
	}
	var pCmd = []string{"/opt/spark/bin/spark-submit"}

	pCmd = append(pCmd, "--deploy-mode", t.tpcdsOptions.DeployMode)

	if t.tpcdsOptions.GenData {
		pCmd = append(pCmd, "--class", "org.apache.kyuubi.tpcds.DataGenerator")
	} else {
		pCmd = append(pCmd, "--class", "org.apache.kyuubi.tpcds.benchmark.RunBenchmark")
	}

	pCmd = append(pCmd, "--conf", fmt.Sprintf("spark.kyuubi.kubernetes.namespace=%s", t.tpcdsOptions.NS),
		"--conf", fmt.Sprintf("spark.kubernetes.executor.podNamePrefix=%s", DefaultTPCDSPrefix),
		"--conf", fmt.Sprintf("spark.master=k8s://%s", config.Host))

	if t.tpcdsOptions.DeployMode == SparkDeployModeCluster {
		pCmd = append(pCmd, "--conf", fmt.Sprintf("spark.kubernetes.driver.pod.name=%s", DefaultTPCDSPrefix+SparkDriverNameSuffix),
			"--conf", fmt.Sprintf("spark.kubernetes.file.upload.path=%s", t.tpcdsOptions.ResultsDir),
			"--conf", fmt.Sprintf("spark.kubernetes.authenticate.driver.serviceAccountName=%s", GenThriftServiceAccountName(t.tpcdsOptions.Name)),
			"--conf", fmt.Sprintf("spark.kubernetes.driver.label.cluster=%s", t.tpcdsOptions.Name),
			"--conf", fmt.Sprintf("spark.kubernetes.driver.label.app=%s", DefaultTPCDSAPP),
			"--conf",
			fmt.Sprintf("spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName=OnDemand"),
			"--conf",
			fmt.Sprintf("spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass=%s", t.tpcdsOptions.StorageClass),
			"--conf",
			fmt.Sprintf("spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit=%dGi", t.tpcdsOptions.ShuffleDiskSize),
			"--conf",
			fmt.Sprintf("spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path=/opt/spark/mnt/dir1"),
			"--conf",
			fmt.Sprintf("spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-local-dir-1.mount.readOnly=false"))
	}

	for i := 0; i < t.tpcdsOptions.ShuffleDisks; i++ {
		pCmd = append(pCmd, "--conf",
			fmt.Sprintf("spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-%d.options.claimName=OnDemand", i+1),
			"--conf",
			fmt.Sprintf("spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-%d.options.storageClass=%s", i+1, t.tpcdsOptions.StorageClass),
			"--conf",
			fmt.Sprintf("spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-%d.options.sizeLimit=%dGi", i+1, t.tpcdsOptions.ShuffleDiskSize),
			"--conf",
			fmt.Sprintf("spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-%d.mount.path=/opt/spark/mnt/dir%d", i+1, i+1),
			"--conf",
			fmt.Sprintf("spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-%d.mount.readOnly=false", i+1))
	}

	if t.tpcdsOptions.ExecutorCores != 0 {
		pCmd = append(pCmd, "--conf", fmt.Sprintf("spark.kubernetes.executor.request.cores=%d", t.tpcdsOptions.ExecutorCores),
			"--conf", fmt.Sprintf("spark.kubernetes.executor.limit.cores=%d", t.tpcdsOptions.ExecutorCores))
	}

	pCmd = append(pCmd,
		"--conf", fmt.Sprintf("spark.kubernetes.executor.label.cluster=%s", t.tpcdsOptions.Name),
		"--conf", fmt.Sprintf("spark.kubernetes.executor.label.app=%s", DefaultTPCDSAPP))

	if t.tpcdsOptions.DeployMode == SparkDeployModeCluster {
		if t.tpcdsOptions.DriverMemory != 0 {
			pCmd = append(pCmd, "--driver-memory", fmt.Sprintf("%dG", t.tpcdsOptions.DriverMemory))
		}
		if t.tpcdsOptions.DriverCores != 0 {
			pCmd = append(pCmd, "--driver-cores", fmt.Sprintf("%d", t.tpcdsOptions.DriverCores))
		}
	}
	if t.tpcdsOptions.Executors != 0 {
		pCmd = append(pCmd, "--num-executors", fmt.Sprintf("%d", t.tpcdsOptions.Executors))
	}

	if t.tpcdsOptions.ExecutorMemory != 0 {
		pCmd = append(pCmd, "--executor-memory", fmt.Sprintf("%dG", t.tpcdsOptions.ExecutorMemory))
	}

	if t.tpcdsOptions.ExecutorCores != 0 {
		pCmd = append(pCmd, "--executor-cores", fmt.Sprintf("%d", t.tpcdsOptions.ExecutorCores))
	}

	pCmd = append(pCmd, fmt.Sprintf("/opt/kyuubi/jars/%s", t.tpcdsOptions.TPCDSJar))
	pCmd = append(pCmd, "--db", t.tpcdsOptions.DataBase)

	if t.tpcdsOptions.GenData {
		pCmd = append(pCmd, "--scaleFactor", fmt.Sprintf("%d", t.tpcdsOptions.ScaleFactor))
		pCmd = append(pCmd, "--parallel", fmt.Sprintf("%d", t.tpcdsOptions.Parallel))
	} else {
		pCmd = append(pCmd, "--iterations", fmt.Sprintf("%d", t.tpcdsOptions.Iterations))
		if t.tpcdsOptions.Queries != nil {
			pCmd = append(pCmd, "--filter", fmt.Sprintf("%s", t.tpcdsOptions.Queries))
		}
		pCmd = append(pCmd, "--results-dir", fmt.Sprintf("%s", t.tpcdsOptions.ResultsDir))
	}
	_, err = runExecCommand(podName, t.tpcdsOptions.NS, t.tpcdsOptions.TTY, pCmd)
	if err != nil {
		return err
	}
	return nil
}

func (t *tpcdsCmd) run(_ []string) error {
	return t.runTPCDS()
}
