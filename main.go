package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
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

const batch = 5000

type batchResult struct {
	err error
	successCounter int
	failCounter int
	filteredCount int
	count int
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
			panic("tidb connect error")
		}

		tCnt, err := tableCnt(mysql, "stmts")
		if err != nil {
			panic(err)
		}

		offset := 0
		batchNum := 0
		resCh := make(chan *batchResult)
		for offset < tCnt {
			sql := fmt.Sprintf("select * from stmts limit %d,%d",
				offset, batch)

			// ensure one session per goroutine
			conn, err := tidb.Conn(context.Background())
			if err != nil {
				panic(err)
			}

			go batchCompare(mysql, conn, offset, sql, resCh)

			offset += batch
			batchNum++
		}

		counter := 0
		successCounter := 0
		failCounter := 0
		filteredCount := 0

		for i := 0; i < batchNum; i++ {
			res := <- resCh
			if res.err != nil {
				panic(res.err)
			} else {
				counter += res.count
				successCounter += res.successCounter
				failCounter += res.failCounter
				filteredCount += res.filteredCount
			}
		}

		log.Printf(`
Summary:
	total: %d
	success count: %d
	fail count: %d
	filtered count: %d
`, counter, successCounter, failCounter, filteredCount)



	},
}

type compareParam struct {
	source string
	stdout string
	result string
}

func getAllResult(mysql *sql.DB, stmt string) ([]*compareParam, error)  {
	rows, err := mysql.Query(stmt)
	if err != nil {
		return nil, err
	}

	var id int
	var uid, result, source, stdout string

	compares := make([]*compareParam, 0)

	for rows.Next() {
		if rows.Err() != nil {
			rows.Close()
			return nil, rows.Err()
		}
		rows.Scan(&id, &uid, &result, &source, &stdout)
		compares = append(compares, &compareParam{source, stdout, result})
	}

	return compares, nil
}

func batchCompare(mysql *sql.DB, tiConn *sql.Conn, offset int, stmtSql string, resCh chan *batchResult) {
	defer tiConn.Close()

	rows, err := getAllResult(mysql, stmtSql)
	if err != nil {
		resCh <- &batchResult{err:errors.WithStack(err)}
		return
	}

	logger, file, err := getLogger(offset)
	if err != nil {
		resCh <- &batchResult{err:errors.WithStack(err)}
		return
	}

	defer file.Close()

	tempdb := fmt.Sprintf("playground%d", offset)

	counter := 0
	successCounter := 0
	failCounter := 0
	filteredCount := 0

	for index, param := range rows {
		rowNum := offset + index
		logger.Printf("starting execute %d sql\n", rowNum)

		consistent, filtered := compare(tiConn, param.source, param.stdout,
			param.result, tempdb)
		if filtered {
			filteredCount++
		} else if consistent {
			logger.Printf("%d sql compare end, success\n", rowNum)
			successCounter++
		} else {
			logger.Println("result not consistent sqls:")
			logger.Println(param.source)
			logger.Printf("%d sql compare end, fail\n", rowNum)
			failCounter++
		}

		counter++
	}

	logger.Printf("playground test offset %d ok\n", offset)
	logger.Printf(`Summary:
	success count: %d
	fail count: %d
	filtered count: %d
`,
		successCounter, failCounter, filteredCount)

	resCh <- &batchResult{
		successCounter:successCounter,
		failCounter:failCounter,
		filteredCount:filteredCount,
		count:counter,
	}
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

func compare(tiConn *sql.Conn, source string, expected string, result string, tempdb string) (consistent bool,
	filtered bool) {

	if filter(source, "show databases",
		"select version()", "schema", "database", "use mysql", "desc",
		"select now()", "explain") {
		return false, true
	}

	if result == "timeout" || result == "" {
		return false, true
	}

	_, err := tiConn.ExecContext(context.Background(),
		fmt.Sprintf("drop database if exists %s", tempdb))
	if err != nil {
		return false, false
	}

	_, err = tiConn.ExecContext(context.Background(),
		fmt.Sprintf("create database %s", tempdb))
	if err != nil {
		return false, false
	}

	_, err = tiConn.ExecContext(context.Background(),
		fmt.Sprintf("use %s", tempdb))
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

		rows, err := tiConn.QueryContext(context.Background(), line)
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


func getLogger(offset int) (*log.Logger, *os.File, error) {
	f, err := os.OpenFile(
		fmt.Sprintf("playground-%d.log", offset),
		os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, err
	}
	logger := log.New(f, "", log.LstdFlags)
	return logger, f, nil
}

func tableCnt(db *sql.DB, table string) (int, error) {
	rows, err := db.Query(
		fmt.Sprintf("select count(*) from %s", table))
	if err != nil {
		return 0, err
	}

	rows.Next()
	var cnt int
	rows.Scan(&cnt)

	return cnt, nil
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
