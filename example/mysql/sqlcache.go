package main

import (
	"database/sql"
	"fmt"
	"redis-cache/cache"
	"redis-cache/sqlcache"
	"redis-cache/utils"
	"strings"
	"time"
)

type (
	User struct {
		Id         int64     `db:"id"`
		Name       string    `db:"name"`
		Password   string    `db:"password"`
		Mobile     string    `db:"mobile"`
		Gender     string    `db:"gender"`
		Nickname   string    `db:"nickname"`
		CreateTime time.Time `db:"create_time"`
		UpdateTime time.Time `db:"update_time"`
	}

	UserModel struct {
		sqlcache.CachedConn
		table string
	}
)

var (
	userFieldNames          = utils.FieldNames(&User{}, "db")
	userRows                = strings.Join(userFieldNames, ",")
	userRowsExpectAutoSet   = strings.Join(utils.RemoveStrs(userFieldNames, "`id`", "`create_time`", "`update_time`"), ",")
	userRowsWithPlaceHolder = strings.Join(utils.RemoveStrs(userFieldNames, "`id`", "`create_time`", "`update_time`"), "=?,") + "=?"
	cacheBookPrefix         = "cache#Book#book#"
	cachepricePrefix        = "cache#Book#price#"
	cacheUserIdPrefix       = "cache#User#id#"
	cacheUserNamePrefix     = "cache#User#name#"
	cacheUserMobilePrefix   = "cache#User#mobile#"
)

func NewUserModel(conn sqlcache.SqlConn, c cache.CacheConf) *UserModel {
	return &UserModel{
		CachedConn: sqlcache.NewConn(conn, c),
		table:      "`user`",
	}
}

func (m *UserModel) Insert(data User) (sql.Result, error) {
	userNameKey := fmt.Sprintf("%s%v", cacheUserNamePrefix, data.Name)
	userMobileKey := fmt.Sprintf("%s%v", cacheUserMobilePrefix, data.Mobile)
	ret, err := m.Exec(
		func(conn sqlcache.SqlConn) (result sql.Result, err error) {
			query := fmt.Sprintf("insert into %s (%s) values (?, ?, ?, ?, ?)", m.table, userRowsExpectAutoSet)
			return conn.Exec(query, data.Name, data.Password, data.Mobile, data.Gender, data.Nickname)
		},
		userNameKey,
		userMobileKey,
	)
	return ret, err
}

func (m *UserModel) FindOne(id int64) (*User, error) {
	userIdKey := fmt.Sprintf("%s%v", cacheUserIdPrefix, id)
	var resp User
	err := m.QueryRow(
		&resp,
		userIdKey, func(conn sqlcache.SqlConn, v interface{}) error {
			query := fmt.Sprintf("select %s from %s where `id` = ? limit 1", userRows, m.table)
			return conn.QueryRow(v, query, id)
		},
	)
	switch err {
	case nil:
		return &resp, nil
	case sqlcache.ErrNotFound:
		return nil, sqlcache.ErrNotFound
	default:
		return nil, err
	}
}

func (m *UserModel) FindOneByName(name string) (*User, error) {
	userNameKey := fmt.Sprintf("%s%v", cacheUserNamePrefix, name)
	var resp User
	err := m.QueryRowIndex(
		&resp,
		userNameKey,
		m.formatPrimary,
		func(conn sqlcache.SqlConn, v interface{}) (i interface{}, e error) {
			query := fmt.Sprintf("select %s from %s where `name` = ? limit 1", userRows, m.table)
			if err := conn.QueryRow(&resp, query, name); err != nil {
				return nil, err
			}
			return resp.Id, nil
		},
		m.queryPrimary,
	)
	switch err {
	case nil:
		return &resp, nil
	case sqlcache.ErrNotFound:
		return nil, sqlcache.ErrNotFound
	default:
		return nil, err
	}
}

func (m *UserModel) Update(data User) error {
	userIdKey := fmt.Sprintf("%s%v", cacheUserIdPrefix, data.Id)
	_, err := m.Exec(
		func(conn sqlcache.SqlConn) (result sql.Result, err error) {
			query := fmt.Sprintf("update %s set %s where `id` = ?", m.table, userRowsWithPlaceHolder)
			return conn.Exec(query, data.Name, data.Password, data.Mobile, data.Gender, data.Nickname, data.Id)
		},
		userIdKey,
	)
	return err
}

func (m *UserModel) Delete(id int64) error {
	data, err := m.FindOne(id)
	if err != nil {
		return err
	}

	userIdKey := fmt.Sprintf("%s%v", cacheUserIdPrefix, id)
	userNameKey := fmt.Sprintf("%s%v", cacheUserNamePrefix, data.Name)
	userMobileKey := fmt.Sprintf("%s%v", cacheUserMobilePrefix, data.Mobile)
	_, err = m.Exec(
		func(conn sqlcache.SqlConn) (result sql.Result, err error) {
			query := fmt.Sprintf("delete from %s where `id` = ?", m.table)
			return conn.Exec(query, id)
		},
		userIdKey,
		userNameKey,
		userMobileKey,
	)
	return err
}

func (m *UserModel) formatPrimary(primary interface{}) string {
	return fmt.Sprintf("%s%v", cacheUserIdPrefix, primary)
}

func (m *UserModel) queryPrimary(conn sqlcache.SqlConn, v, primary interface{}) error {
	query := fmt.Sprintf("select %s from %s where `id` = ? limit 1", userRows, m.table)
	return conn.QueryRow(v, query, primary)
}

func main() {
	// cf := cache.ClusterConf{
	// 	cache.NodeConf{
	// 		Host:   "127.0.0.1:6379",
	// 		Type:   "node",
	// 		Weight: 100,
	// 	},
	// }
	cf := cache.ClusterConf{
		cache.NodeConf{
			Host:   "127.0.0.1:7000",
			Type:   "cluster",
			Weight: 100,
		},
		cache.NodeConf{
			Host:   "127.0.0.1:7001",
			Type:   "cluster",
			Weight: 100,
		},
		cache.NodeConf{
			Host:   "127.0.0.1:7002",
			Type:   "cluster",
			Weight: 100,
		},
		cache.NodeConf{
			Host:   "127.0.0.1:7003",
			Type:   "cluster",
			Weight: 100,
		},
		cache.NodeConf{
			Host:   "127.0.0.1:7004",
			Type:   "cluster",
			Weight: 100,
		},
		cache.NodeConf{
			Host:   "127.0.0.1:7005",
			Type:   "cluster",
			Weight: 100,
		},
	}

	db := sqlcache.NewMysql("root:123456@tcp(localhost:3306)/mydb?parseTime=true")
	user := NewUserModel(db, cf)

	ret, err := user.Insert(User{
		Name:     "slue",
		Password: "0988990",
		Mobile:   "12232233",
		Gender:   "men",
		Nickname: "bigs",
	})

	if err != nil {
		fmt.Printf("inster err: %v \n", err)
	}
	fmt.Printf("inster result: %v \n", ret)

	res, err := user.FindOneByName("slue")
	if err != nil {
		fmt.Printf("FindOneByName err: %v\n", err)
	}
	fmt.Printf("FindOneByName result: %v\n", res)

}
