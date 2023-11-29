package cmd

import (
	"context"
	"github.com/beltran/gohive"
	"github.com/spf13/cobra"
	"io"
	"log"
)

const (
	sqlDesc    = `'sql' command execute sql commond on the NineCluster`
	sqlExample = ` kubectl nine sql c1 --namespace c1-ns`
)

type SqlOptions struct {
	Name string
	NS   string
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
	return cmd
}

func (s *sqlCmd) validate(args []string) error {
	s.sqlOpts.Name = args[0]
	return ValidateClusterArgs("sql", args)
}

func (s *sqlCmd) run(_ []string) error {
	//svcName := s.sqlOpts.Name + DefaultNineSuffix + "-kyuubi"
	svcName := "10.110.221.87"
	conf := gohive.NewConnectConfiguration()
	conn, err := gohive.Connect(svcName, 10009, "NONE", conf)
	if err != nil {
		return err
	}
	cursor := conn.Cursor()
	cursor.Exec(context.TODO(), "show databases")
	if cursor.Err != nil {
		return cursor.Err
	}
	var str string
	for cursor.HasMore(context.TODO()) {
		cursor.FetchOne(context.TODO(), &str)
		if cursor.Err != nil {
			log.Fatal(cursor.Err)
		}
		log.Println(str)
	}
	cursor.Close()
	conn.Close()
	return nil
}
