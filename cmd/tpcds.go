package cmd

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"io"
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
	Name           string
	NS             string
	GenData        bool
	DataBase       string
	TPCDSJar       string
	ScaleFactor    int
	Parallel       int
	Iterations     int
	Executors      int
	ExecutorMemory int
	ExecutorCores  int
	Queries        []string
	ResultsDir     string
	StorageClass    string
	ShuffleDiskSize int
	TTY             bool
	//EnableGluten    bool
	//OffHeapSize     int
	//EnableAQE       bool
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
	f.IntVar(&c.tpcdsOptions.ExecutorMemory, "executor-memory", 0, "the memory of the spark executor for the TPC-DS")
	f.IntVar(&c.tpcdsOptions.ExecutorCores, "executor-cores", 0, "the cores of the spark executor for the TPC-DS")
	f.StringVar(&c.tpcdsOptions.StorageClass, "storageclass", "directpv-min-io", "storageclass fo tpcds")
	f.IntVar(&c.tpcdsOptions.ShuffleDiskSize, "shuffle-size", 200, "shuffle disk size of executor")
	//f.BoolVarP(&c.tpcdsOptions.EnableAQE, "enable-aqe", "a", false, "enable AQE")
	//f.BoolVarP(&c.tpcdsOptions.EnableGluten, "enable-gluten", "v", false, "enable gluten")
	//f.IntVar(&c.tpcdsOptions.OffHeapSize, "offheap-size", 20, "offheap memory size")
	f.BoolVar(&c.tpcdsOptions.TTY, "tty", false, "enable tty")
	f.BoolVar(&DEBUG, "debug", false, "debug mode")
	return cmd
}

func (t *tpcdsCmd) validate(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("not enough parameters")
	}
	t.tpcdsOptions.Name = args[0]
	return ValidateClusterArgs("tpcds", args)
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
	var pCmd = []string{"/opt/spark/bin/spark-submit"}
	if t.tpcdsOptions.GenData {
		pCmd = append(pCmd, "--class", "org.apache.kyuubi.tpcds.DataGenerator")
	} else {
		pCmd = append(pCmd, "--class", "org.apache.kyuubi.tpcds.benchmark.RunBenchmark")
	}
	pCmd = append(pCmd, "--conf", fmt.Sprintf("spark.kyuubi.kubernetes.namespace=%s", t.tpcdsOptions.NS),
		"--conf", "spark.kubernetes.executor.podNamePrefix=tpcds-spark",
		"--conf", fmt.Sprintf("spark.master=k8s://%s", config.Host),
		"--conf", "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName=OnDemand",
		"--conf", fmt.Sprintf("spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass=%s", t.tpcdsOptions.StorageClass),
		"--conf", fmt.Sprintf("spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit=%dGi", t.tpcdsOptions.ShuffleDiskSize),
		"--conf", "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path=/opt/spark/mnt",
		"--conf", "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.readOnly=false")
	if t.tpcdsOptions.ExecutorCores != 0 {
		pCmd = append(pCmd, "--conf", fmt.Sprintf("spark.kubernetes.executor.request.cores=%d", t.tpcdsOptions.ExecutorCores),
			"--conf", fmt.Sprintf("spark.kubernetes.executor.limit.cores=%d", t.tpcdsOptions.ExecutorCores))
	}
	//AQE enabled by default
	//if t.tpcdsOptions.EnableAQE {
	//	pCmd = append(pCmd, "--conf", "spark.sql.adaptive.enabled=true",
	//		"--conf", "spark.sql.adaptive.forceApply=false",
	//		"--conf", "spark.sql.adaptive.logLevel=info",
	//		"--conf", "spark.sql.adaptive.advisoryPartitionSizeInBytes=256m",
	//		"--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
	//		"--conf", "spark.sql.adaptive.coalescePartitions.minPartitionNum=1",
	//		"--conf", "spark.sql.adaptive.coalescePartitions.initialPartitionNum=8192",
	//		"--conf", "spark.sql.adaptive.fetchShuffleBlocksInBatch=true",
	//		"--conf", "spark.sql.adaptive.localShuffleReader.enabled=true",
	//		"--conf", "spark.sql.adaptive.skewJoin.enabled=true",
	//		"--conf", "spark.sql.adaptive.skewJoin.skewedPartitionFactor=5",
	//		"--conf", "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=400m",
	//		"--conf", "spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin=0.2",
	//		"--conf", "spark.sql.adaptive.optimizer.excludedRules",
	//		"--conf", "spark.sql.autoBroadcastJoinThreshold=-1")
	//}
	//many problems in gluten now
	//if t.tpcdsOptions.EnableGluten {
	//	pCmd = append(pCmd, "--conf", "spark.plugins=io.glutenproject.GlutenPlugin",
	//		"--conf", "spark.memory.offHeap.enabled=true",
	//		"--conf", fmt.Sprintf("spark.memory.offHeap.size=%dg", t.tpcdsOptions.OffHeapSize),
	//		"--conf", "spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager",
	//	)
	//}
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
	err = runExecCommand(podName, t.tpcdsOptions.NS, t.tpcdsOptions.TTY, pCmd)
	if err != nil {
		return err
	}
	return nil
}

func (t *tpcdsCmd) run(_ []string) error {
	return t.runTPCDS()
}
