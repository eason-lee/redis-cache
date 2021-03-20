package sqlcache

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type (
	txExec interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
	}
	trans interface {
		txExec
		Commit() error
		Rollback() error
	}

	txSession struct {
		*sql.Tx
	}
	beginnable func(*sqlx.DB) (trans, error)
)

func (t txSession) Exec(q string, args ...interface{}) (sql.Result, error) {
	return t.Tx.Exec(q, args...)
}

func begin(db *sqlx.DB) (trans, error) {
	if tx, err := db.Begin(); err != nil {
		return nil, err
	} else {
		return txSession{
			Tx: tx,
		}, nil
	}
}

func transact(db *commonSqlConn, b beginnable, fn func(txExec) error) (err error) {
	conn, err := getSqlConn(db.driverName, db.datasource)
	if err != nil {
		logInstanceError(db.datasource, err)
		return err
	}

	return transactOnConn(conn, b, fn)
}

func transactOnConn(conn *sqlx.DB, b beginnable, fn func(txExec) error) (err error) {
	var tx trans
	tx, err = b(conn)
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("recover from %#v, rollback failed: %s", p, e)
			} else {
				err = fmt.Errorf("recoveer from %#v", p)
			}
		} else if err != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("transaction failed: %s, rollback failed: %s", err, e)
			}
		} else {
			err = tx.Commit()
		}
	}()

	return fn(tx)
}
