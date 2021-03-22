package sqlcache

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

var ErrNotFound = sql.ErrNoRows

type (
	Session interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
		QueryRow(v interface{}, query string, args ...interface{}) error
		QueryRows(v interface{}, query string, args ...interface{}) error
	}

	SqlConn interface {
		Session
		Transact(func(txExec) error) error
	}

	SqlOption func(*commonSqlConn)

	// thread-safe
	commonSqlConn struct {
		driverName string
		datasource string
		beginTx    beginnable
		accept     func(error) bool
	}
)

func NewSqlConn(driverName, datasource string, opts ...SqlOption) SqlConn {
	conn := &commonSqlConn{
		driverName: driverName,
		datasource: datasource,
		beginTx:    begin,
	}
	for _, opt := range opts {
		opt(conn)
	}

	return conn
}

func (db *commonSqlConn) Exec(q string, args ...interface{}) (result sql.Result, err error) {
	var conn *sqlx.DB
	conn, err = getSqlConn(db.driverName, db.datasource)
	if err != nil {
		logInstanceError(db.datasource, err)
		return
	}

	result, err = conn.Exec(q, args...)

	return
}

func (db *commonSqlConn) QueryRow(v interface{}, q string, args ...interface{}) error {
	conn, err := getSqlConn(db.driverName, db.datasource)
	if err != nil {
		logInstanceError(db.datasource, err)
		return err
	}
	return conn.Get(v, q, args...)

}

func (db *commonSqlConn) QueryRows(v interface{}, q string, args ...interface{}) error {
	conn, err := getSqlConn(db.driverName, db.datasource)
	if err != nil {
		logInstanceError(db.datasource, err)
		return err
	}
	return conn.Select(v, q, args...)

}

func (db *commonSqlConn) Transact(fn func(txExec) error) error {
	return transact(db, db.beginTx, fn)
}
