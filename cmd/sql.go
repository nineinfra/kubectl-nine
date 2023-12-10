package cmd

import (
	"context"
	"fmt"
	"github.com/beltran/gohive"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"io"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	"os"
)

const (
	sqlDesc    = `'sql' command execute sql commond on the NineCluster`
	sqlExample = `1. Create database
   $ kubectl nine sql c1 --namespace c1-ns "create database test"

2. Create table
   $ kubectl nine sql c1 --namespace c1-ns "create table test.test(id int,name string)"

3. Insert to table
   $ kubectl nine sql c1 --namespace c1-ns "insert into table test.test values(1,\"nineinfa\")"

4. Select table
   $ kubectl nine sql c1 --namespace c1-ns "select * from test.test"

5. Show tables
   $ kubectl nine sql c1 --namespace c1-ns "show tables from test"`
)

type SqlOptions struct {
	Name      string
	NS        string
	Statement string
}

type sqlCmd struct {
	out     io.Writer
	errOut  io.Writer
	output  bool
	sqlOpts SqlOptions
}

func newClusterSqlCmd(out io.Writer, errOut io.Writer) *cobra.Command {
	c := &sqlCmd{out: out, errOut: errOut}

	cmd := &cobra.Command{
		Use:     "sql <NINECLUSTERNAME>",
		Short:   "Execute sql on a NineCluster",
		Long:    sqlDesc,
		Example: sqlExample,
		Args: func(cmd *cobra.Command, args []string) error {
			return c.validate(args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := c.run(args)
			if err != nil {
				return err
			}
			return nil
		},
	}
	cmd = DisableHelp(cmd)
	f := cmd.Flags()
	f.StringVarP(&c.sqlOpts.NS, "namespace", "n", "", "k8s namespace for this ninecluster")
	f.StringVarP(&c.sqlOpts.Statement, "statement", "s", "show databases", "simple sql statement")
	return cmd
}

func (s *sqlCmd) validate(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("Not enough parameters!")
	}
	s.sqlOpts.Name = args[0]
	return ValidateClusterArgs("sql", args)
}

func (s *sqlCmd) getThriftIpAndPort() (string, int32) {
	svcName := s.sqlOpts.Name + DefaultNineSuffix + "-kyuubi"
	path, _ := rootCmd.Flags().GetString(kubeconfig)
	client, err := GetKubeClient(path)
	if err != nil {
		return "", 0
	}
	svc, err := client.CoreV1().Services(s.sqlOpts.NS).Get(context.TODO(), svcName, metav1.GetOptions{})
	if err != nil {
		return "", 0
	}
	var thriftIP string
	var thriftPort int32
	switch svc.Spec.Type {
	case corev1.ServiceTypeClusterIP:
		thriftIP = svc.Spec.ClusterIP
		for _, v := range svc.Spec.Ports {
			if v.Name == DefaultThriftPortName {
				thriftPort = v.Port
			}
		}
	case corev1.ServiceTypeNodePort:
		config, err := clientcmd.BuildConfigFromFlags("", path)
		if err != nil {
			return "", 0
		}
		thriftIP = config.Host
		for _, v := range svc.Spec.Ports {
			if v.Name == DefaultThriftPortName {
				thriftPort = v.NodePort
			}
		}
	}
	return thriftIP, thriftPort
}

func (s *sqlCmd) run(_ []string) error {
	thriftIP, thriftPort := s.getThriftIpAndPort()
	if thriftIP == "" || thriftPort == 0 {
		return errors.New("Invalid Thrift Access Info!")
	}
	conf := gohive.NewConnectConfiguration()
	conn, err := gohive.Connect(thriftIP, int(thriftPort), "NONE", conf)
	if err != nil {
		return err
	}
	defer conn.Close()

	cursor := conn.Cursor()
	defer cursor.Close()

	cursor.Exec(context.TODO(), s.sqlOpts.Statement)
	if cursor.Err != nil {
		return cursor.Err
	}

	row1 := cursor.RowMap(context.TODO())
	if row1 != nil {
		var header []string
		var data1 []string
		for k, v := range row1 {
			header = append(header, k)
			data1 = append(data1, fmt.Sprintf("%v", v))
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader(header)
		table.Append(data1)

		for cursor.HasMore(context.TODO()) {
			row := cursor.RowMap(context.TODO())
			var data []string
			for _, v := range header {
				data = append(data, fmt.Sprintf("%v", row[v]))
			}
			table.Append(data)
		}
		table.SetBorder(true)
		table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
		table.SetAlignment(tablewriter.ALIGN_LEFT)
		table.Render()
	}
	return nil
}
