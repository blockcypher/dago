package dago

import (
	"github.com/gocql/gocql"
)

type CassandraDb struct {
	session *gocql.Session
	helper  *CQLHelper
	da      *DataAccess
}

// Convenience method to initiate a session and return a CassandraDb
// with a default cluster configuration.
func Open(keyspace string, hosts ...string) (*CassandraDb, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return Wrap(session), nil
}

// Wraps an existing gocql session into a CassandraDb to gain access
// to our CQL helper and Data Access object
func Wrap(session *gocql.Session) *CassandraDb {
	helper := NewCQLHelper(session)
	da := NewDataAccess(helper)
	store := &CassandraDb{session, helper, da}
	return store
}

// Return a DataAccess reference. DataAccess is the highest level facility to
// manipulate data from/to Cassandra
func (self *CassandraDb) GetDA() *DataAccess {
	return self.da
}

// Return a CQLHelper reference. The CQL helper is an intermediate level tool that
// smoothes out the wrinkles in generate CQL statement strings.
func (self *CassandraDb) GetHelper() *CQLHelper {
	return self.helper
}

// Return the gocql Session reference
func (self *CassandraDb) GetSession() *gocql.Session {
	return self.session
}

// Close the underlying connection.
func (self *CassandraDb) Close() {
	self.session.Close()
}
