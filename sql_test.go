package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExecSql(t *testing.T) {
	myConfig := &Config{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "123456",
	}

	mdb, err := openDBWithRetry("mysql", myConfig.DSN(), 10)
	assert.Equal(t, nil, err)

	defer mdb.Close()

	rows, err := mdb.Query("create database haha;")
	assert.Equal(t, nil, err)

	cols, err := rows.Columns()
	assert.Equal(t, nil, err)

	fmt.Println(cols)
	fmt.Println(rows.Next())
}

func TestCompare(t *testing.T) {
	sql := `
create table emp(emp_id integer, name varchar(100), dept_id integer);
create table dept(dept_id integer, name varchar(100));
insert into emp values(1, "emp1", 1);
insert into emp values(2, "emp2", 1);
insert into emp values(3, "emp3", 2);
insert into emp values(4, "emp4", 3);
insert into dept values(1, "あああ");
insert into dept values(3, "いいい");
insert into dept values(4, "ううう");
select e.name, d.name from emp e right join dept d on e.dept_id = d.dept_id order by e.name;
`

	tiConfig := &Config{
		Host: "127.0.0.1",
		Port: 4000,
		User: "root",
	}
	tidb, err := openDBWithRetry("mysql", tiConfig.DSN(), 10)
	assert.Equal(t, nil, err)

	consistent, filtered, err := compare(tidb, sql, "NULL\tううう\nemp1\tあああ\nemp2\tあああ\nemp4\tいいい\n",
		"success", "playtest")

	assert.Equal(t, nil, err)
	assert.Equal(t, false, filtered)
	assert.Equal(t, true, consistent)
}

func TestDblCnt(t *testing.T) {
	myConfig := &Config{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "123456",
		DB:       "crawl",
	}
	mysql, err := openDBWithRetry("mysql", myConfig.DSN(), 10)
	assert.Equal(t, nil, err)
	cnt, err := tableCnt(mysql, "stmts")
	assert.Equal(t, nil, err)

	fmt.Println(cnt)
}
