package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native" // Native engine

	"os"
	"testing"
)

func TestExecSql(t *testing.T) {
	myConfig := &Config{
		Host: "localhost",
		Port: 3306,
		User: "root",
		Password:"123456",
	}

	mdb := openDB(myConfig)

	defer mdb.Close()

	_, _, err := mdb.Query("create database haha;")
	assert.Equal(t, nil, err)
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
		Host:"127.0.0.1",
		Port:4000,
		User:"root",
	}
	tidb := openDB(tiConfig)

	consistent, filtered := compare(tidb, sql, "NULL\tううう\nemp1\tあああ\nemp2\tあああ\nemp4\tいいい\n",
		"success")

	assert.Equal(t, false, filtered)
	assert.Equal(t, true, consistent)
}

func TestMyMysql(t *testing.T) {
	db := mysql.New("tcp", "", "127.0.0.1:4000", user, "",
		"")

	err := db.Connect()
	if err != nil {
		panic(err)
	}

	db.Use("test")

	rows, res, err := db.Query("select * from x where id > %d", 20)
	if err != nil {
		panic(err)
	}

	for _, row := range rows {
		fmt.Println("=============")

		for _, col := range row {
			if col == nil {
				// col has NULL value
			} else {
				// Do something with text in col (type []byte)
			}
		}
		// You can get specific value from a row
		val1 := row[1].([]byte)

		// You can use it directly if conversion isn't needed
		os.Stdout.Write(val1)

		// You can get converted value
		number := row.Int(0)      // Zero value
		str    := row.Str(1)      // First value
		fmt.Printf("id: %d\n", number)
		fmt.Printf("name: %s\n", str)

		// You may get values by column name
		first := res.Map("id")
		second := res.Map("name")
		id, name := row.Int(first), row.Str(second)
		fmt.Printf("id: %d\n", id)
		fmt.Printf("name: %s\n", name)
	}
}
