package dago

import (
	"strconv"
	"strings"

	"github.com/gocql/gocql"
)

type F struct {
	Name  string
	Value interface{}
}

type CQLHelper struct {
	db *gocql.Session
}

func NewCQLHelper(db *gocql.Session) *CQLHelper {
	return &CQLHelper{db: db}
}

func (self *CQLHelper) Get(table string, pk *F, fields ...string) *gocql.Query {
	q := "select " + strings.Join(fields, ", ") + " from " + table + " where " + pk.Name + "=?"
	return self.db.Query(q, pk.Value)
}

func (self *CQLHelper) Get2(table string, pk1 *F, pk2 *F, fields ...string) *gocql.Query {
	q := "select " + strings.Join(fields, ", ") + " from " + table +
		" where " + pk1.Name + "=? and " + pk2.Name + "=?"
	return self.db.Query(q, pk1.Value, pk2.Value)
}

func (self *CQLHelper) Get3(table string, pk1 *F, pk2 *F, pk3 *F, fields ...string) *gocql.Query {
	q := "select " + strings.Join(fields, ", ") + " from " + table +
		" where " + pk1.Name + "=? and " + pk2.Name + "=? and " + pk3.Name + "=?"
	return self.db.Query(q, pk1.Value, pk2.Value, pk3.Value)
}

func (self *CQLHelper) GetN(table string, pks []*F, fields ...string) *gocql.Query {
	keys, values := self.andKeysAndValues(pks...)
	q := "select " + strings.Join(fields, ", ") + " from " + table + " where " + keys
	return self.db.Query(q, values...)
}

func (self *CQLHelper) GetNLimit(table string, limit int, pks []*F, fields ...string) *gocql.Query {
	keys, values := self.andKeysAndValues(pks...)
	q := "select " + strings.Join(fields, ", ") + " from " + table + " where " + keys +
		" limit " + strconv.Itoa(limit)
	return self.db.Query(q, values...)
}

func (self *CQLHelper) GetNLimitFilterBeforeBlockHeight(table string, limit int, beforeBH uint, pks []*F, fields ...string) *gocql.Query {
	keys, values := self.andKeysAndValues(pks...)
	strBeforeBH := strconv.Itoa(int(beforeBH))
	q := "select " + strings.Join(fields, ", ") + " from " + table + " where " + keys + " and bheight<=" + strBeforeBH +
		" limit " + strconv.Itoa(limit)
	return self.db.Query(q, values...)
}

func (self *CQLHelper) GetNLimitFilterAfterBlockHeight(table string, limit int, beforeBH uint, pks []*F, fields ...string) *gocql.Query {
	keys, values := self.andKeysAndValues(pks...)
	strBeforeBH := strconv.Itoa(int(beforeBH))
	q := "select " + strings.Join(fields, ", ") + " from " + table + " where " + keys + " and bheight>=" + strBeforeBH +
		" limit " + strconv.Itoa(limit)
	return self.db.Query(q, values...)
}

func (self *CQLHelper) GetNLimitFilterBlockHeights(table string, limit int, beforeBH, afterBH uint, pks []*F, fields ...string) *gocql.Query {
	keys, values := self.andKeysAndValues(pks...)
	strBeforeBH := strconv.Itoa(int(beforeBH))
	strAfterBH := strconv.Itoa(int(afterBH))
	q := "select " + strings.Join(fields, ", ") + " from " + table + " where " + keys + " and bheight<=" + strBeforeBH +
		" and bheight>=" + strAfterBH + " limit " + strconv.Itoa(limit)
	return self.db.Query(q, values...)
}

func (self *CQLHelper) Save(table string, fields ...*F) error {
	return self.save(table, false, fields...).Consistency(gocql.LocalQuorum).Exec()
}

func (self *CQLHelper) SaveIfNotExists(table string, fields ...*F) *gocql.Query {
	return self.save(table, true, fields...)
}

func (self *CQLHelper) save(table string, ine bool, fields ...*F) *gocql.Query {
	keys := fields[0].Name
	qs := "?"
	values := make([]interface{}, len(fields))
	values[0] = fields[0].Value

	for n := 1; n < len(fields); n++ {
		keys += "," + fields[n].Name
		qs += ", ?"
		values[n] = fields[n].Value
	}
	q := "insert into " + table + " (" + keys + ") values (" + qs + ")"
	if ine {
		q += " if not exists"
	}
	return self.db.Query(q, values...)
}

func (self *CQLHelper) Save2If(table string, cond *F, pk1 *F, pk2 *F, fields ...*F) *gocql.Query {
	keys, values := self.commaKeysAndValues(fields...)
	q := "update " + table + " set " + keys +
		" where " + pk1.Name + " = ? and " + pk2.Name + " = ? if " + cond.Name + " = ?"
	values = append(values, pk1.Value, pk2.Value, cond.Value)
	return self.db.Query(q, values...)
}

func (self *CQLHelper) FullScan(table string, fields ...string) *gocql.Iter {
	q := "select " + strings.Join(fields, ", ") + " from " + table
	return self.db.Query(q).PageSize(2000).Consistency(gocql.LocalOne).Iter()
}

func (self *CQLHelper) FullScanQuorum(table string, fields ...string) *gocql.Iter {
	q := "select " + strings.Join(fields, ", ") + " from " + table
	return self.db.Query(q).PageSize(2000).Consistency(gocql.Quorum).Iter()
}

func (self *CQLHelper) Fetch(table string, limit int, pk []*F, fields ...string) *gocql.Iter {
	q := "select " + strings.Join(fields, ", ") + " from " + table +
		" where "

	q += pk[0].Name + "=? "
	params := make([]interface{}, len(pk))
	params[0] = pk[0].Value

	for k, field := range pk[1:] {
		q += " and " + field.Name + "=? "
		params[k+1] = field.Value
	}
	q += " limit " + strconv.Itoa(limit)

	return self.db.Query(q, params...).PageSize(2000).Consistency(gocql.LocalQuorum).Iter()
}

func (self *CQLHelper) Scan(table string, limit int, pk *F, fields ...string) *gocql.Iter {
	q := "select " + strings.Join(fields, ", ") + " from " + table +
		" where " + pk.Name + "=? limit " + strconv.Itoa(limit)
	return self.db.Query(q, pk.Value).PageSize(2000).Consistency(gocql.LocalQuorum).Iter()
}

func (self *CQLHelper) Scan2(table string, limit int, pk *F, pk2 *F, fields ...string) *gocql.Iter {
	q := "select " + strings.Join(fields, ", ") + " from " + table +
		" where " + pk.Name + "=? and " + pk2.Name + "=? limit " + strconv.Itoa(limit)
	return self.db.Query(q, pk.Value, pk2.Value).PageSize(2000).Consistency(gocql.LocalQuorum).Iter()
}

func (self *CQLHelper) Query(q string, params ...interface{}) *gocql.Iter {
	return self.db.Query(q, params...).Consistency(gocql.LocalQuorum).PageSize(2000).Iter()
}

func (self *CQLHelper) Delete(table string, kvs ...*F) error {
	keys, values := self.andKeysAndValues(kvs...)
	return self.db.Query("delete from "+table+" where "+keys, values...).Consistency(gocql.LocalQuorum).Exec()
}

func (self *CQLHelper) DeleteBy(table string, id string, value interface{}) error {
	q := "delete from " + table + " where " + id + "=?"
	return self.db.Query(q, value).Consistency(gocql.LocalQuorum).Exec()
}

func queryValues(q *gocql.Query, n int) ([]interface{}, error) {
	sl := make([]interface{}, n)
	// error is same as iterator error returned on close
	iter := q.Iter()
	iter.Scan(sl...)
	return sl, iter.Close()
}

func (self *CQLHelper) andKeysAndValues(ks ...*F) (string, []interface{}) {
	return self.sepKeysAndValues(" and ", ks...)
}

func (self *CQLHelper) commaKeysAndValues(ks ...*F) (string, []interface{}) {
	return self.sepKeysAndValues(", ", ks...)
}

func (self *CQLHelper) sepKeysAndValues(sep string, kvs ...*F) (string, []interface{}) {
	keys := ""
	values := make([]interface{}, len(kvs))

	for n, field := range kvs {
		keys += field.Name + " = ?"
		if n < len(kvs)-1 {
			keys += sep
		}
		values[n] = field.Value
	}
	return keys, values
}

// Utility function to eliminate not found errors
func ENF(err error) error {
	if err == gocql.ErrNotFound {
		return nil
	} else {
		return err
	}
}
