package dago

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type SimpleDao struct {
	AString   string    `column:"astring,key"`
	SomeBytes []byte    `column:"some_bytes,key"`
	ABigUInt  uint64    `column:"abigint,sort"`
	AnInt     int64     `column:"anint,sort"`
	ATime     time.Time `column:"some_date_time"`
	ABigInt   *big.Int  `column:"avarint"`
	ABool     bool      `column:"abool"`
}

func (self *SimpleDao) TableName() string {
	return "simple_dao"
}

func TestReflect(t *testing.T) {
	simple := &SimpleDao{"foo", []byte{42, 101}, 123, 11, time.Unix(42, 1), big.NewInt(42), true}
	// obviously don't use any method that relies on a Session here
	da := NewDataAccess(nil)
	assert.Equal(t, da.FieldNamesOfKind(simple, ANY), []string{"AString", "SomeBytes", "ABigUInt", "AnInt", "ATime", "ABigInt", "ABool"})
	assert.Equal(t, da.ColNamesOfKind(simple, ANY), []string{"astring", "some_bytes", "abigint", "anint", "some_date_time", "avarint", "abool"})
	assert.Equal(t, da.Keys(simple), []*F{&F{"astring", "foo"}, &F{"some_bytes", []byte{42, 101}}, &F{"abigint", uint64(123)}, &F{"anint", int64(11)}})
}
