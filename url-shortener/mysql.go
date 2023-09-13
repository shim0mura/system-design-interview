package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/schemalex/schemalex"
	"github.com/schemalex/schemalex/diff"
)

func generateAlter(conf *mysql.Config) (string, error) {
	diffSql := bytes.Buffer{}
	err := diff.Sources(&diffSql, schemalex.NewMySQLSource(conf.FormatDSN()), schemalex.NewLocalFileSource("./schema/table.sql"))
	return diffSql.String(), err
}

type Url struct {
	Id          int64  `db:"id"`
	OriginalUrl string `db:"original_url"`
	Token       string `db:"token"`
}

type UrlRepository struct {
	db *sqlx.DB
}

func NewUrlRepository() UrlRepository {
	u := UrlRepository{}

	var config = &mysql.Config{
		User:                 "root",
		Passwd:               "root",
		Net:                  "tcp",
		Addr:                 "127.0.0.1:3307",
		DBName:               "url_shortener",
		MultiStatements:      true,
		ParseTime:            true,
		AllowNativePasswords: true,
	}

	db, err := sqlx.Connect("mysql", config.FormatDSN())
	if err != nil {
		log.Fatalln("failed to connect database in migration:", err)
	}
	u.db = db

	sql, err := generateAlter(config)
	if err != nil {
		log.Fatalln("failed to generate alter sql:", err)
	}
	fmt.Println("--- migration ---")
	fmt.Println(sql)
	fmt.Println("--- migration ---")

	if sql == "" {
		fmt.Println("skip migration due to migration is not needed")
	} else {
		_, err := u.db.Exec(sql)
		if err != nil {
			log.Fatalln("failed to migration:", err)
		}
		fmt.Println("migration success")
	}

	// multistatementはmigrationのときのみ有効にする
	u.db.Close()
	config.MultiStatements = false

	u.db, err = sqlx.Connect("mysql", config.FormatDSN())
	if err != nil {
		log.Fatal("failed to connect database")
	}
	return u
}

func (u *UrlRepository) SaveUrl(url *Url) (int64, error) {
	result, err := u.db.NamedExec("insert into url(original_url, token) values (:original_url, :token) ON DUPLICATE KEY UPDATE updated_at = now(), token = :token", url)
	if err != nil {
		log.Println("url save failed", err)
		return 0, err
	}
	lastInsertId, err := result.LastInsertId()
	if err != nil {
		log.Println("get last insert id failed", err)
		return 0, err
	}
	return lastInsertId, nil
}
func (u *UrlRepository) UpdateUrl(url *Url) (int64, error) {
	result, err := u.db.NamedExec("update url set original_url = :original_url, token = :token where id = :id", url)
	if err != nil {
		log.Println("url save failed", err)
		return 0, err
	}
	lastInsertId, err := result.LastInsertId()
	if err != nil {
		log.Println("get last insert id failed", err)
		return 0, err
	}
	return lastInsertId, nil
}

func (u *UrlRepository) GetUrlFromToken(token string) (*Url, error) {
	url := &Url{}
	err := u.db.Get(url, "SELECT original_url, token FROM url WHERE token = ?", token)

	if err != nil {
		if err != sql.ErrNoRows {
			return nil, err
		} else {
			return url, nil
		}
	} else {
		return url, nil
	}
}

func (u *UrlRepository) GetTokenFromUrl(originalUrl string) (*Url, error) {
	url := &Url{}
	err := u.db.Get(url, "SELECT original_url, token FROM url WHERE original_url = ?", originalUrl)

	if err != nil {
		if err != sql.ErrNoRows {
			return nil, err
		} else {
			return url, nil
		}
	} else {
		return url, nil
	}
}
