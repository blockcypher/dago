package dago

import (
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/gocql/gocql"
)

// The DAO name reminds of old Java nightmares. Let's hope this goes better.

// Type of the key in case of composite keys
type colKind byte

const (
	ANY colKind = iota // both any and non kinds only used for filtering
	ANY_KEY
	NON_KEY
	// key types below
	PARTITION_KEY
	CLUSTERING_KEY
)

/**
 * All DAOs must implement this interface in addition to having each field that needs to
 * be persisted annotated with `column:"{colum_nname}"`.
 */
type DAOLite interface {
	TableName() string
}

// Pre save hook that a DAO can optionally implement. Will be called before every save
// operation.
type DAOPreHook interface {
	PreSave()
}
type DAOPostSaveHook interface {
	PostSave()
}
type DAOPostHook interface {
	PostLoad()
}

type DataAccess struct {
	helper *CQLHelper

	fieldDefsCache map[string][]*fieldDef
	defMutex       *sync.RWMutex
}

// field definition for a DAO, cached by type name to avoid recomputing
// on each operation
type fieldDef struct {
	pos  int // field index in the struct
	name string
	col  string
	kind colKind
}

func (self *fieldDef) String() string {
	return strconv.Itoa(self.pos) + ". " + self.name + " " + self.col
}

func NewDataAccess(helper *CQLHelper) *DataAccess {
	return &DataAccess{helper, make(map[string][]*fieldDef), new(sync.RWMutex)}
}

// Saves a new row or updates an existing one using all field values for the provided DAO.
func (self *DataAccess) Save(dao DAOLite) error {
	return self.SaveTable(dao.TableName(), dao)
}

// Same as save but allows overriding the table name
func (self *DataAccess) SaveTable(tableName string, dao DAOLite) error {
	if daopre, ok := dao.(DAOPreHook); ok {
		daopre.PreSave()
	}
	params := self.Fields(dao)
	res := self.helper.Save(tableName, params...)
	if daopost, ok := dao.(DAOPostSaveHook); ok {
		daopost.PostSave()
	}
	return res
}

// Saves a new or updates an existing one using all primary key values as well as the value
// of provided fields. Fields are simply the string name of the corresponding  DAO struct
// field.
func (self *DataAccess) SavePartial(dao DAOLite, fields ...string) error {
	if daopre, ok := dao.(DAOPreHook); ok {
		daopre.PreSave()
	}
	params := append(self.Keys(dao), self.fieldsOfKind(dao, NON_KEY, fields)...)
	return self.helper.Save(dao.TableName(), params...)
}

// Accepts a DAO with primary keys fields set and gets the corresponding row, setting the
// remaining fields appropriately. The DAO is updated in place and also returned for
// convenience.
// Example:
//   user, err := da.Get(&User{Country: "US", SSN: "890-123-4567"})
func (self *DataAccess) Get(dao DAOLite) (DAOLite, error) {
	return self.GetBy(self.Keys(dao), dao)
}

// Gets a DAO using the provided keys instead of inferring the keys from the DAO annotations.
func (self *DataAccess) GetBy(keys []*F, dao DAOLite) (DAOLite, error) {
	return self.GetByTable(dao.TableName(), keys, dao)
}

func (self *DataAccess) GetByTable(table string, keys []*F, dao DAOLite) (DAOLite, error) {
	colsToGet := self.ColNamesOfKind(dao, NON_KEY)
	fieldsToGet := self.FieldNamesOfKind(dao, NON_KEY)
	values := self.fieldsZeroValuesArray(dao, fieldsToGet)

	iter := self.helper.GetN(table, keys, colsToGet...).Iter()
	found := iter.Scan(values...)
	if err := iter.Close(); err != nil {
		return nil, err
	}
	if !found {
		return nil, gocql.ErrNotFound
	}
	self.setFieldsValues(dao, fieldsToGet, values)

	if daopost, ok := dao.(DAOPostHook); ok {
		daopost.PostLoad()
	}
	return dao, nil
}

// Creates an iterator to go over all rows stored under given partition keys. The provided DAO
// instance is expected to have values for those keys. Combine with the Next method for full
// iteration.
// Example:
//   user := &User{Country: "US", State: "CA"}
//   iter := da.PartitionIter(user)
//   for da.Next(iter, user) {...}
//   iter.Close()
func (self *DataAccess) PartitionIter(dao DAOLite) *gocql.Iter {
	colsToGet := append(self.ColNamesOfKind(dao, NON_KEY), self.ColNamesOfKind(dao, CLUSTERING_KEY)...)
	q := self.helper.GetN(dao.TableName(), self.PartitionKeys(dao), colsToGet...)
	return q.PageSize(2000).Consistency(gocql.LocalQuorum).Iter()
}

func (self *DataAccess) PartitionIterLimit(dao DAOLite, limit int) *gocql.Iter {
	colsToGet := append(self.ColNamesOfKind(dao, NON_KEY), self.ColNamesOfKind(dao, CLUSTERING_KEY)...)
	q := self.helper.GetNLimit(dao.TableName(), limit, self.PartitionKeys(dao), colsToGet...)
	return q.PageSize(2000).Consistency(gocql.LocalQuorum).Iter()
}

func (self *DataAccess) PartitionIterLimitFilterBeforeBlockHeight(dao DAOLite, limit int, blockHeight uint) *gocql.Iter {
	colsToGet := append(self.ColNamesOfKind(dao, NON_KEY), self.ColNamesOfKind(dao, CLUSTERING_KEY)...)
	q := self.helper.GetNLimitFilterBeforeBlockHeight(dao.TableName(), limit, blockHeight, self.PartitionKeys(dao), colsToGet...)
	return q.PageSize(2000).Consistency(gocql.LocalQuorum).Iter()
}

func (self *DataAccess) PartitionIterLimitFilterBlockHeights(dao DAOLite, limit int, beforeBH, afterBH uint) *gocql.Iter {
	colsToGet := append(self.ColNamesOfKind(dao, NON_KEY), self.ColNamesOfKind(dao, CLUSTERING_KEY)...)
	q := self.helper.GetNLimitFilterBlockHeights(dao.TableName(), limit, beforeBH, afterBH, self.PartitionKeys(dao), colsToGet...)
	return q.PageSize(2000).Consistency(gocql.LocalQuorum).Iter()
}

func (self *DataAccess) FullIter(dao DAOLite) *gocql.Iter {
	colsToGet := append(self.ColNamesOfKind(dao, ANY), self.ColNamesOfKind(dao, CLUSTERING_KEY)...)
	return self.helper.FullScan(dao.TableName(), colsToGet...)
}

// See PartitionIter
func (self *DataAccess) Next(iter *gocql.Iter, dao DAOLite) bool {
	fieldsToGet := append(self.FieldNamesOfKind(dao, NON_KEY), self.FieldNamesOfKind(dao, CLUSTERING_KEY)...)
	values := self.fieldsZeroValuesArray(dao, fieldsToGet)
	next := iter.Scan(values...)
	if !next {
		return false
	}
	self.setFieldsValues(dao, fieldsToGet, values)
	if daopost, ok := dao.(DAOPostHook); ok {
		daopost.PostLoad()
	}
	return true
}

func (self *DataAccess) fieldsZeroValuesArray(dao DAOLite, fieldNames []string) []interface{} {
	v := reflect.ValueOf(dao).Elem()
	values := make([]interface{}, 0, len(fieldNames))
	for _, field := range fieldNames {
		values = append(values, reflect.New(v.FieldByName(field).Type()).Interface())
	}
	return values
}

func (self *DataAccess) setFieldsValues(dao DAOLite, fieldNames []string, values []interface{}) {
	v := reflect.ValueOf(dao).Elem()
	valn := 0
	for _, field := range fieldNames {
		sf := v.FieldByName(field)
		if values[valn] != nil {
			valof := reflect.ValueOf(values[valn])
			sf.Set(valof.Elem())
		} else {
			sf.Set(reflect.Zero(sf.Type()))
		}
		valn++
	}
}

func (self *DataAccess) Delete(dao DAOLite) error {
	return self.helper.Delete(dao.TableName(), self.Keys(dao)...)
}

// All column values filters (column/value pairs) for the provided DAO
func (self *DataAccess) Fields(dao interface{}) []*F {
	return self.fieldsOfKind(dao, ANY, []string{})
}

// Primary keys values filters (column/value pairs) for the provided DAO
func (self *DataAccess) Keys(dao interface{}) []*F {
	return self.fieldsOfKind(dao, ANY_KEY, []string{})
}

// Partition keys values filters (column/value pairs) for the provided DAO
func (self *DataAccess) PartitionKeys(dao interface{}) []*F {
	return self.fieldsOfKind(dao, PARTITION_KEY, []string{})
}

func (self *DataAccess) fieldsOfKind(dao interface{}, filter colKind, names []string) []*F {
	def := self.initFieldsDefs(dao)
	v := reflect.ValueOf(dao).Elem()
	fields := make([]*F, 0, len(def))
	for _, fdef := range def {
		if filter == ANY || filter == ANY_KEY &&
			(fdef.kind >= PARTITION_KEY || fdef.kind == filter) || filter == fdef.kind {

			if len(names) == 0 || StringInList(fdef.name, names) {
				val := v.FieldByName(fdef.name)
				switch val.Kind() {
				case reflect.Int:
					vali := val.Int()
					fields = append(fields, &F{fdef.col, vali})
				case reflect.Uint:
					vali := val.Uint()
					fields = append(fields, &F{fdef.col, vali})
				case reflect.Float32:
					valf := val.Float()
					fields = append(fields, &F{fdef.col, valf})
				case reflect.String:
					vals := val.String()
					fields = append(fields, &F{fdef.col, vals})
				default:
					fields = append(fields, &F{fdef.col, val.Interface()})
				}
			}
		}
	}
	return fields
}

func (self *DataAccess) FieldNamesOfKind(dao interface{}, filter colKind) []string {
	return self.namesOfKind(dao, false, filter)
}

func (self *DataAccess) ColNamesOfKind(dao interface{}, filter colKind) []string {
	return self.namesOfKind(dao, true, filter)
}

func (self *DataAccess) namesOfKind(dao interface{}, colNotName bool, filter colKind) []string {
	names := make([]string, 0, 5)
	def := self.initFieldsDefs(dao)
	for _, fdef := range def {
		if filter == ANY ||
			filter == ANY_KEY && (fdef.kind >= PARTITION_KEY || fdef.kind == filter) ||
			filter == fdef.kind {

			if colNotName {
				names = append(names, fdef.col)
			} else {
				names = append(names, fdef.name)
			}
		}
	}
	return names
}

func (self *DataAccess) initFieldsDefs(dao interface{}) []*fieldDef {
	// This reliably gets the name of the object
	tname := reflect.TypeOf(dao).String()
	self.defMutex.RLock()
	def := self.fieldDefsCache[tname]
	self.defMutex.RUnlock()
	if def == nil {
		def = fieldDefs(dao)
		self.defMutex.Lock()
		self.fieldDefsCache[tname] = def
		self.defMutex.Unlock()
	}
	return def
}

func fieldDefs(dao interface{}) []*fieldDef {
	t := reflect.TypeOf(dao).Elem()
	fDefs := make([]*fieldDef, 0, t.NumField())
	for n := 0; n < t.NumField(); n++ {
		sf := t.Field(n)
		colspec := strings.Split(sf.Tag.Get("column"), ",")
		colkind := NON_KEY
		if len(colspec) > 1 {
			switch colspec[1] {
			case "key":
				colkind = PARTITION_KEY
			case "sort":
				colkind = CLUSTERING_KEY
			case "traverse":
				sfval := reflect.ValueOf(dao).Elem().Field(n)
				fDefs = append(fDefs, fieldDefs(sfval.Interface())...)
				continue
			default:
				if sf.Anonymous {
					continue
				} else {
					panic("Bad column tag qualifier: " + colspec[1])
				}
			}
		}
		fDefs = append(fDefs, &fieldDef{n, sf.Name, colspec[0], colkind})
	}
	return fDefs
}

// Get rid of this when it's part of the standard library with generics
func StringInList(ts string, l []string) bool {
	for _, s := range l {
		if s == ts {
			return true
		}
	}
	return false
}
