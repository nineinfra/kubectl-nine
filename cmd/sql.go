package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/beltran/gohive"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"io"
	"os"
)

const (
	sqlDesc    = `'sql' command execute sql commond on the NineCluster`
	sqlExample = `1. Create database
   $ kubectl nine sql c1 --namespace c1-ns -s "create database test"

2. Create table
   $ kubectl nine sql c1 --namespace c1-ns -s "create table test.test(id int,name string)"

3. Insert to table
   $ kubectl nine sql c1 --namespace c1-ns -s "insert into table test.test values(1,\"nineinfa\")"

4. Select table
   $ kubectl nine sql c1 --namespace c1-ns -s "select * from test.test"

5. Show tables
   $ kubectl nine sql c1 --namespace c1-ns -s "show tables from test"`
)

type SqlOptions struct {
	Name      string
	NS        string
	TTY       bool
	Silent    bool
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
				fmt.Println(GiveSuggestionsByError(err))
				return err
			}
			return nil
		},
	}
	cmd = DisableHelp(cmd)
	f := cmd.Flags()
	f.StringVarP(&c.sqlOpts.NS, "namespace", "n", "", "k8s namespace for this ninecluster")
	f.StringVar(&DefaultAccessHost, "access-host", "", "access host ip for out cluster access,such as web access")
	f.BoolVar(&c.sqlOpts.TTY, "tty", false, "interactive SQL operation")
	f.BoolVar(&c.sqlOpts.Silent, "silent", true, "be more silent")
	f.StringVarP(&c.sqlOpts.Statement, "statement", "s", "show databases", "simple sql statement")
	return cmd
}

func (s *sqlCmd) validate(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("not enough parameters")
	}
	s.sqlOpts.Name = args[0]
	return ValidateClusterArgs("sql", args)
}

func (s *sqlCmd) interactiveSQL() error {
	podName, err := GetThriftPodName(s.sqlOpts.Name, s.sqlOpts.NS)
	if err != nil {
		return err
	}
	thriftIP, thriftPort := GetThriftIpAndPort(s.sqlOpts.Name, s.sqlOpts.NS)
	if thriftIP == "" || thriftPort == 0 {
		return errors.New("invalid Thrift Access Info")
	}
	pBeelineCmd := []string{"/opt/kyuubi/bin/beeline",
		"-u", fmt.Sprintf("jdbc:hive2://%s:%d", thriftIP, thriftPort),
		"--silent", fmt.Sprintf("%v", s.sqlOpts.Silent)}
	_, err = runExecCommand(podName, s.sqlOpts.NS, true, pBeelineCmd)
	if err != nil {
		return err
	}
	return nil
}

func (s *sqlCmd) directSQL() error {
	thriftIP, thriftPort := GetThriftIpAndPort(s.sqlOpts.Name, s.sqlOpts.NS)
	if thriftIP == "" || thriftPort == 0 {
		return errors.New("invalid Thrift Access Info")
	}
	conf := gohive.NewConnectConfiguration()
	conn, err := gohive.Connect(thriftIP, int(thriftPort), "NONE", conf)
	if err != nil {
		return err
	}
	defer func(conn *gohive.Connection) {
		err := conn.Close()
		if err != nil {
			fmt.Printf("Error: %v \n", err)
		}
	}(conn)

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

func (s *sqlCmd) run(_ []string) error {
	if !s.sqlOpts.TTY {
		return s.directSQL()
	} else {
		return s.interactiveSQL()
	}
}
