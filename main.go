package main

import (
	"bytes"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
	"log"
	"os"
	"strings"
)

var (
	port int
	host string
	passwd string
	user string
	tiPort int
	tiHost string
	tiPasswd string
	tiUser string
)

var rootCmd = &cobra.Command{
	Use:"playground test",
	Short:"test playground sql",
	Run: func(cmd *cobra.Command, args []string) {
		myConfig := &Config{
			Host:host,
			Port:port,
			User: user,
			Password:passwd,
			DB:"crawl",
		}
		mysql := openDB(myConfig)

		tiConfig := &Config{
			Host:tiHost,
			Port:tiPort,
			User:tiUser,
			Password:tiPasswd,
		}
		tidb := openDB(tiConfig)

		rows, res, err := mysql.Query("select * from stmts")
		if err != nil {
			panic(err)
		}

		logger, file, err := getLogger()
		if err != nil {
			panic("open log file error")
		}

		defer file.Close()

		successCounter := 0
		failCounter := 0
		filteredCount := 0

		fmt.Printf("total rows: %d\n", len(rows))

		for id, row := range rows {
			source := row.Str(res.Map("source"))
			stdout := row.Str(res.Map("stdout"))
			result := row.Str(res.Map("result"))

			consistent, filtered := compare(tidb, source, stdout, result)
			if filtered {
				filteredCount++
			} else if consistent {
				log.Printf("%d sql compare end, success\n", id)
				successCounter++
			} else {
				logger.Printf("%d result not consistent:\n", id)
				logger.Println(source)
				log.Printf("%d sql compare end, fail\n", id)
				failCounter++
			}
		}

		log.Println("playground test ok")
		logger.Printf("Summary:\n\tsuccess count: %d\n\tfail count: %d\n\tfiltered count: %d",
			successCounter, failCounter, filteredCount)
	},
}

func filter(source string, filterStrs ... string) bool {

	lowerSource := strings.ToLower(source)

	for _, filterStr := range filterStrs {
		if strings.Contains(lowerSource, filterStr) {
			return true
		}
	}

	return false
}

func compare(tidb mysql.Conn, source string, expected string, result string) (consistent bool,
	filtered bool) {

	if filter(source, "show databases",
		"select version()", "create schema", "use mysql", "desc",
		"select now()", "explain") {
		return false, true
	}

	if result == "timeout" {
		return false, true
	}

	_, _, err := tidb.Query("drop database if exists playground")
	if err != nil {
		return false, false
	}

	_, _, err = tidb.Query("create database playground")
	if err != nil {
		return false, false
	}

	err = tidb.Use("playground")
	if err != nil {
		return false, false
	}

	lines := strings.Split(source, "\n")

	buf := &bytes.Buffer{}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		rows, _, err := tidb.Query(line)
		if err != nil {
			if result != "failure" {
				return false, false
			} else {
				return true, false
			}
		}

		if res := getRowsContent(rows); res != ""{
			buf.WriteString(res)
		}
	}

	real := buf.String()

	if expected != "" && expected != real {
		return false, false
	}

	return true, false
}

func getRowsContent(rows []mysql.Row) string {

	buf := &bytes.Buffer{}
	for _, row := range rows {
		for index, col := range row {
			var colStr string
			if col == nil {
				colStr = "NULL"
			} else {
				colStr = string(col.([]byte))
			}

			if index == 0 {
				buf.WriteString(colStr)
			} else {
				buf.WriteString("\t" + colStr)
			}
		}

		buf.WriteString("\n")
	}

	return buf.String()
}

func init() {
	rootCmd.Flags().IntVarP(&port, "port", "P", 3306, "mysql port")
	rootCmd.Flags().StringVarP(&host, "host", "H", "localhost", "mysql host")
	rootCmd.Flags().StringVar(&passwd, "passwd", "", "mysql password")
	rootCmd.Flags().StringVarP(&user, "user", "U", "root", "mysql user")
	rootCmd.Flags().IntVar(&tiPort, "tiport", 4000, "tidb prot")
	rootCmd.Flags().StringVar(&tiHost, "tihost", "localhost", "tidb host")
	rootCmd.Flags().StringVar(&tiPasswd, "tipasswd", "", "tidb password")
	rootCmd.Flags().StringVar(&tiUser, "tiuser", "root", "tidb user")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
		os.Exit(1)
	}
}

// openDBWithRetry opens a database specified by its database driver name and a
// driver-specific data source name. And it will do some retries if the connection fails.
func openDB(config *Config) mysql.Conn {
	conn := mysql.New("tcp", "",
		fmt.Sprintf("%s:%d", config.Host, config.Port),
		config.User, config.Password, config.DB)

	err := conn.Connect()
	if err != nil {
		panic(err)
	}

	return conn
}


func getLogger() (*log.Logger, *os.File, error) {
	f, err := os.OpenFile("playground.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, err
	}
	logger := log.New(f, "", log.LstdFlags)
	return logger, f, nil
}


type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DB       string
}

func (c *Config) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.User, c.Password, c.Host, c.Port, c.DB)
}

func (c *Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
