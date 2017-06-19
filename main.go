package main

import (
	"context"
	"fmt"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/nakaji-s/gohbase"
	"github.com/nakaji-s/gohbase/filter"
	"github.com/nakaji-s/gohbase/hrpc"
	"gopkg.in/urfave/cli.v1"
	"io"
	"strings"
)

func main() {
	app := cli.NewApp()
	app.Name = "hbase-cmd"
	cli.AppHelpTemplate = `NAME:
   {{.Name}}
USAGE:
   {{.HelpName}} {{if .VisibleFlags}}[global options]{{end}}{{if .Commands}} command [command options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}
   {{if len .Authors}}
AUTHOR:
   {{range .Authors}}{{ . }}{{end}}
   {{end}}{{if .Commands}}
COMMANDS:
{{range .Commands}}{{if not .HideHelp}}   {{join .Names ", "}}{{ "\t"}}{{.Usage}}{{ "\n" }}{{end}}{{end}}{{end}}{{if .VisibleFlags}}
GLOBAL OPTIONS:
   {{range .VisibleFlags}}{{.}}
   {{end}}{{end}}{{if .Copyright }}
COPYRIGHT:
   {{.Copyright}}
   {{end}}{{if .Version}}
VERSION:
   {{.Version}}
   {{end}}`
	cli.CommandHelpTemplate = `NAME:
   {{.HelpName}}

USAGE:
   {{.HelpName}}{{if .VisibleFlags}} [command options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}{{if .Category}}

CATEGORY:
   {{.Category}}{{end}}{{if .Description}}

DESCRIPTION:
   {{.Description}}{{end}}{{if .VisibleFlags}}

OPTIONS:
   {{range .VisibleFlags}}{{.}}
   {{end}}{{end}}`
	app.Version = "0.1.0"
	app.Commands = []cli.Command{
		{
			Name:     "create",
			Category: "template",
			Action:   create,
			Flags: []cli.Flag{
				cli.StringFlag{Name: "cf", Value: "cf", Usage: "columnFamilies exmaple=cf1,cf2"},
			},
		},
		{
			Name:     "drop",
			Category: "template",
			Action:   drop,
		},
		{
			Name:     "put",
			Category: "template",
			Action:   put,
			Flags: []cli.Flag{
				cli.StringFlag{Name: "cf", Value: "cf", Usage: "columnFamily"},
				cli.StringFlag{Name: "cq", Value: "cq", Usage: "columnQualifier"},
				cli.StringFlag{Name: "key"},
				cli.StringFlag{Name: "value"},
			},
		},
		{
			Name:     "delete",
			Category: "template",
			Action:   delete,
			Flags: []cli.Flag{
				cli.StringFlag{Name: "key"},
			},
		},
		{
			Name:     "get",
			Category: "template",
			Action:   get,
			Flags: []cli.Flag{
				cli.StringFlag{Name: "key"},
				cli.StringFlag{Name: "format", Value: "key,cfcq,value",
					Usage: "key(rowkey), cfcq(columnFamily:columnQualifier), value(value), em(epoc milli), ts(timestamp)"},
			},
		},
		{
			Name:     "scan",
			Category: "template",
			Action:   scan,
			Flags: []cli.Flag{
				cli.StringFlag{Name: "prefix", Value: "", Usage: "prefixFilter"},
				cli.StringFlag{Name: "format", Value: "key,cfcq,value",
					Usage: "key(rowkey), cfcq(columnFamily:columnQualifier), value(value), em(epoc milli), ts(timestamp)"},
			},
		},
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "zookeeper-quorum", Value: "localhost"},
		cli.StringFlag{Name: "hbase-node", Value: "/hbase-unsecure", Usage: "node path stored in zookeeper"},
		cli.StringFlag{Name: "table", Value: "system:dummyTable", Usage: "namespace:tableName"},
		cli.StringFlag{Name: "log", Value: "warn", Usage: "log level for stdout debug/info/warn/error/fatal"},
	}
	app.EnableBashCompletion = true

	app.Before = func(c *cli.Context) error {
		// set LogLevel
		level, _ := log.ParseLevel(c.GlobalString("log"))
		log.SetLevel(level)
		return nil
	}

	// run
	app.Run(os.Args)
}

func create(c *cli.Context) error {
	tablename := []byte(c.GlobalString("table"))
	cFamilies := strings.Split(c.String("cf"), ",")
	adminClient := gohbase.NewAdminClient(
		c.GlobalString("zookeeper-quorum"), gohbase.ZookeeperRoot(c.GlobalString("hbase-node")))
	cfmap := map[string]map[string]string{}
	for _, cf := range cFamilies {
		cfmap[cf] = nil
	}
	createTableRequest := hrpc.NewCreateTable(context.Background(), tablename, cfmap)
	err := adminClient.CreateTable(createTableRequest)
	if err != nil {
		return err
	}
	return nil
}

func drop(c *cli.Context) error {
	tablename := []byte(c.GlobalString("table"))
	adminClient := gohbase.NewAdminClient(
		c.GlobalString("zookeeper-quorum"), gohbase.ZookeeperRoot(c.GlobalString("hbase-node")))

	disableTableRequest := hrpc.NewDisableTable(context.Background(), tablename)
	err := adminClient.DisableTable(disableTableRequest)
	if err != nil {
		return err
	}

	deleteTableRequest := hrpc.NewDeleteTable(context.Background(), tablename)
	err = adminClient.DeleteTable(deleteTableRequest)
	if err != nil {
		return err
	}
	return nil
}

func put(c *cli.Context) error {
	client := gohbase.NewClient(
		c.GlobalString("zookeeper-quorum"), gohbase.ZookeeperRoot(c.GlobalString("hbase-node")))
	cFamily := c.String("cf")
	cQualifier := c.String("cq")
	key := c.String("key")
	value := c.String("value")

	cfmap := map[string]map[string][]byte{cFamily: nil}
	cfmap[cFamily] = map[string][]byte{cQualifier: []byte(value)}
	putRequest, _ := hrpc.NewPutStr(context.Background(), c.GlobalString("table"), key, cfmap)
	_, err := client.Put(putRequest)
	if err != nil {
		return err
	}
	return nil
}

func delete(c *cli.Context) error {
	client := gohbase.NewClient(
		c.GlobalString("zookeeper-quorum"), gohbase.ZookeeperRoot(c.GlobalString("hbase-node")))
	key := c.String("key")

	delRequest, _ := hrpc.NewDelStr(context.Background(), c.GlobalString("table"), key, nil)
	_, err := client.Delete(delRequest)
	if err != nil {
		return err
	}
	return nil
}

func get(c *cli.Context) error {
	client := gohbase.NewClient(
		c.GlobalString("zookeeper-quorum"), gohbase.ZookeeperRoot(c.GlobalString("hbase-node")))
	key := c.String("key")
	format := c.String("format")

	getRequest, _ := hrpc.NewGetStr(context.Background(), c.GlobalString("table"), key)
	getRsp, err := client.Get(getRequest)
	if err != nil {
		return err
	}
	for _, cell := range getRsp.Cells {
		str := ""
		for _, s := range strings.Split(format, ",") {
			switch s {
			case "key":
				str += string((cell.Row))
			case "cfcq":
				str += string((cell.Family)) + ":" + string((cell.Qualifier))
			case "value":
				str += string(cell.Value)
			case "qm":
				str += fmt.Sprint(cell.Timestamp)
			case "ts":
				str += fmt.Sprintf("%s", time.Unix(int64(*cell.Timestamp)/1000, 0))
			}
			str += ","
		}
		fmt.Println(str[:len(str)-1])
	}

	return nil
}

func scan(c *cli.Context) error {
	client := gohbase.NewClient(
		c.GlobalString("zookeeper-quorum"), gohbase.ZookeeperRoot(c.GlobalString("hbase-node")))
	prefix := c.String("prefix")
	format := c.String("format")

	scanRequest, err := hrpc.NewScanStr(context.Background(), c.GlobalString("table"),
		hrpc.Filters(filter.NewPrefixFilter([]byte(prefix))))
	if err != nil {
		return err
	}

	scanRsp := client.Scan(scanRequest)
	for {
		result, err := scanRsp.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		for _, cell := range result.Cells {
			str := ""
			for _, s := range strings.Split(format, ",") {
				switch s {
				case "key":
					str += string((cell.Row))
				case "cfcq":
					str += string((cell.Family)) + ":" + string((cell.Qualifier))
				case "value":
					str += string(cell.Value)
				case "qm":
					str += fmt.Sprint(cell.Timestamp)
				case "ts":
					str += fmt.Sprintf("%s", time.Unix(int64(*cell.Timestamp)/1000, 0))
				}
				str += ","
			}
			fmt.Println(str[:len(str)-1])
		}
	}
	return nil
}
