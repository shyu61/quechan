package database

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

var DB *sql.DB

func Connect() *sql.DB {
	user := "user"
	password := "pass"
	host := "localhost"
	port := "3306"
	database_name := "queuechan_development"

	dbconf := user + ":" + password + "@tcp(" + host + ":" + port + ")/" + database_name + "?charset=utf8mb4"
	db, err := sql.Open("mysql", dbconf)
	if err != nil {
		fmt.Println(err.Error())
	}

	return db
}
