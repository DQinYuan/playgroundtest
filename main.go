package main

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	"log"
	"os"
	"strings"
	"time"
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

type compareParam struct {
	source string
	stdout string
	result string
}

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
		mysql, err := openDBWithRetry("mysql", myConfig.DSN(), 10)
		if err != nil {
			panic("mysql connect error")
		}

		tiConfig := &Config{
			Host:tiHost,
			Port:tiPort,
			User:tiUser,
			Password:tiPasswd,
		}
		tidb, err := openDBWithRetry("mysql", tiConfig.DSN(), 10)
		if err != nil {
			panic(err)
		}

		rows, err := mysql.Query("select * from stmts")
		if err != nil {
			panic(err)
		}

		var id int
		var uid, result, source, stdout string

		logger, file, err := getLogger()
		if err != nil {
			panic("open log file error")
		}

		defer file.Close()

		counter := 0
		successCounter := 0
		failCounter := 0
		filteredCount := 0

		compares := make([]*compareParam, 0)

		start := time.Now()

		fmt.Println("Start get all stmts")

		for rows.Next() {
			rows.Scan(&id, &uid, &result, &source, &stdout)
			compares = append(compares, &compareParam{source, stdout, result})
		}

		fmt.Printf("Already get all stmts: %d, time used: %s", len(compares),
			time.Since(start))

		for _, cParam := range compares {
			log.Printf("starting execute %d sql\n", counter)

			consistent, filtered := compare(tidb, cParam.source,
				cParam.stdout, cParam.result)
			if filtered {
				filteredCount++
			} else if consistent {
				log.Printf("%d sql compare end, success\n", counter)
				successCounter++
			} else {
				logger.Println("result not consistent:")
				logger.Println(source)
				log.Printf("%d sql compare end, fail\n", counter)
				failCounter++
			}

			counter++
		}

		if rows.Err() != nil {
			fmt.Println(rows.Err())
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

func compare(tidb *sql.DB, source string, expected string, result string) (consistent bool,
	filtered bool) {

	if filter(source, "show databases",
		"select version()", "create schema", "create database", "use mysql", "desc", "test",
		"select now()", "explain") {
		return false, true
	}

	if result == "timeout" {
		return false, true
	}

	_, err := tidb.Exec("drop database if exists playground")
	if err != nil {
		return false, false
	}

	_, err = tidb.Exec("create database playground")
	if err != nil {
		return false, false
	}

	_, err = tidb.Exec("use playground")
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

		rows, err := tidb.Query(line)
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

		rows.Close()
	}

	real := buf.String()

	if expected != "" && expected != real {
		return false, false
	}

	return true, false
}

func getRowsContent(rows *sql.Rows) string {
	cols, err := rows.Columns()
	if err != nil {
		return ""
	}

	buf := &bytes.Buffer{}
	for rows.Next() {
		var columns = make([][]byte, len(cols))
		var pointer = make([]interface{}, len(cols))
		for i := range columns {
			pointer[i] = &columns[i]
		}
		err := rows.Scan(pointer...)
		if err != nil {
			return ""
		}

		for index, col := range columns {
			colStr := string(col)
			if colStr == "" {colStr = "NULL"}
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
func openDBWithRetry(driverName, dataSourceName string, retryCnt int) (mdb *sql.DB, err error) {
	startTime := time.Now()
	sleepTime := time.Millisecond * 500
	for i := 0; i < retryCnt; i++ {
		mdb, err = sql.Open(driverName, dataSourceName)
		if err != nil {
			fmt.Printf("open db %s failed, retry count %d err %v\n", dataSourceName, i, err)
			time.Sleep(sleepTime)
			continue
		}
		err = mdb.Ping()
		if err == nil {
			break
		}
		fmt.Printf("ping db %s failed, retry count %d err %v\n", dataSourceName, i, err)
		mdb.Close()
		time.Sleep(sleepTime)
	}
	if err != nil {
		fmt.Printf("open db %s failed %v, take time %v\n", dataSourceName, err, time.Since(startTime))
		return nil, err
	}

	return
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
